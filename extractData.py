import streamlit as st
import boto3
import os
import pandas as pd
import traceback # For detailed error logging

# Import botocore exceptions first
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
# Then import the boto3 base exception separately
from boto3.exceptions import Boto3Error


# --- Configuration ---
# Specify the correct AWS region you want to use
# Consider making this configurable via Streamlit sidebar or environment variable
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Define the keys you want to extract (used for ordering and display)
# Using a dictionary allows mapping variations to a canonical display name
TARGET_KEYS_MAP = {
    "Meeting Date": "Meeting Date",
    "Record Date for Notice": "Record Date for Notice",
    "Record Date for Voting": "Record Date for Voting",
    "Beneficial Ownership Determination Date": "Beneficial Ownership Determination Date",
    "Securities entitled to Notice": "Securities entitled to Notice",
    "Securities entitled to Vote": "Securities entitled to Vote",
    "Meeting Type": "Meeting Type",
    "Direct sending of proxy-related materials to NOBOs by issuer": "Direct sending of proxy-related materials to NOBOs by issuer",
    # Map both newline and space versions to the same canonical key
    "Issuer to pay for sending proxy-related materials to OBOs\nby proximate intermediary": "Issuer to pay for sending proxy-related materials to OBOs by proximate intermediary",
    "Issuer to pay for sending proxy-related materials to OBOs by proximate intermediary": "Issuer to pay for sending proxy-related materials to OBOs by proximate intermediary",
    "Notice and Access": "Notice and Access"
}
# List of canonical keys for ordering the final output
ORDERED_DISPLAY_KEYS = [
    "Meeting Date",
    "Record Date for Notice",
    "Record Date for Voting",
    "Beneficial Ownership Determination Date",
    "Securities entitled to Notice",
    "Securities entitled to Vote",
    "Meeting Type",
    "Direct sending of proxy-related materials to NOBOs by issuer",
    "Issuer to pay for sending proxy-related materials to OBOs by proximate intermediary",
    "Notice and Access"
]
# --- End Configuration ---

# --- Helper Functions ---
def get_text_from_block(block, blocks_map):
    """
    Extracts text from a block, handling WORD children and SELECTION_ELEMENTs.

    Args:
        block (dict): The Textract block object.
        blocks_map (dict): A map of block IDs to block objects for efficient lookup.

    Returns:
        str: The extracted text, stripped of leading/trailing whitespace.
    """
    text = ""
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = blocks_map.get(child_id)
                    if not child_block:
                        continue # Should not happen in valid response, but good practice
                    if child_block['BlockType'] == 'WORD':
                        text += child_block.get('Text', '') + ' '
                    elif child_block['BlockType'] == 'SELECTION_ELEMENT':
                         # Represent selection status clearly
                         status = child_block.get('SelectionStatus', 'NOT_SELECTED')
                         text += f"[{status}] "
    # Fallback if the block itself has text (e.g., simple VALUE block)
    # or if it's a KEY/VALUE block without explicit WORD children in relationships
    if not text.strip() and 'Text' in block:
         text = block.get('Text', '')

    return text.strip()

def find_value_block(key_block, blocks_map):
    """
    Finds the value block(s) associated with a key block and extracts their text.

    Args:
        key_block (dict): The Textract block object identified as a KEY.
        blocks_map (dict): A map of block IDs to block objects.

    Returns:
        str: The concatenated text of the associated VALUE block(s).
    """
    value_text = ""
    if 'Relationships' in key_block:
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    value_block = blocks_map.get(value_id)
                    if value_block:
                        # Recursively get text, as VALUE blocks can also have children
                        value_text += get_text_from_block(value_block, blocks_map) + " "
    return value_text.strip()

# --- Core Textract Processing Function ---
def analyze_pdf_with_textract(file_bytes, region, target_keys_config):
    """
    Analyzes a PDF document using AWS Textract FORMS feature, performing
    case-insensitive key matching based on the provided configuration.

    Args:
        file_bytes (bytes): The byte content of the PDF file.
        region (str): The AWS region to use for the Textract client.
        target_keys_config (dict): Dictionary mapping potential key variations
                                   (including case/newlines) to a canonical display key.

    Returns:
        dict: A dictionary mapping canonical display keys to their found values
              (or "Not Found"). Returns None if a critical AWS error occurs.
    """
    try:
        # Initialize Textract client
        textract = boto3.client('textract', region_name=region)

        # Call Textract API
        response = textract.analyze_document(
            Document={'Bytes': file_bytes},
            FeatureTypes=["FORMS"] # Essential for Key-Value pair extraction
        )

        # Process the response
        blocks = response.get('Blocks', [])
        if not blocks:
            st.warning("Textract did not return any blocks for this document.")
            return {} # Return empty dict if no blocks found

        blocks_map = {block['Id']: block for block in blocks}
        extracted_data_raw = {} # Store raw findings {found_key_text: value_text}

        # Create a lookup map for case-insensitive matching:
        # {normalized_lower_key: canonical_display_key}
        normalized_lookup = {
            key.replace('\n', ' ').strip().lower(): display_key
            for key, display_key in target_keys_config.items()
        }

        # Iterate through blocks to find KEY entities
        for block in blocks:
            # Ensure it's a KEY_VALUE_SET block identified as a KEY
            if block.get('BlockType') == "KEY_VALUE_SET" and 'KEY' in block.get('EntityTypes', []):
                # Extract the text content of the KEY block
                key_text = get_text_from_block(block, blocks_map)
                # Normalize the found key for lookup (lowercase, no trailing colon, handle newlines)
                normalized_found_key = key_text.strip().rstrip(':').replace('\n', ' ').strip().lower()

                # Check if this normalized key matches any of our targets
                if normalized_found_key in normalized_lookup:
                    canonical_key = normalized_lookup[normalized_found_key]
                    # Avoid overwriting if found multiple times; first match wins for simplicity
                    # Or potentially append/handle duplicates if needed
                    if canonical_key not in extracted_data_raw:
                        value_text = find_value_block(block, blocks_map)
                        extracted_data_raw[canonical_key] = value_text

        # Prepare final results based on the desired ordered keys
        final_results = {
            display_key: extracted_data_raw.get(display_key, "Not Found")
            for display_key in ORDERED_DISPLAY_KEYS
        }

        return final_results

    # Specific AWS credential errors
    except (NoCredentialsError, PartialCredentialsError):
        st.error("AWS credentials not found or incomplete. Please configure your credentials (e.g., ~/.aws/credentials, environment variables, or IAM role).")
        return None
    # AWS client errors (API issues, permissions, etc.)
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        error_message = e.response.get('Error', {}).get('Message')
        request_id = e.response.get('ResponseMetadata', {}).get('RequestId')
        st.error(f"AWS Client Error ({error_code}): {error_message}\nRequest ID: {request_id}")
        # Optionally log the full traceback for debugging
        # st.code(traceback.format_exc())
        return None
    # General Boto3/Botocore errors (connection issues, etc.)
    except Boto3Error as e:
        st.error(f"AWS SDK Error: {e}")
        # st.code(traceback.format_exc())
        return None
    # Catch any other unexpected errors during processing
    except Exception as e:
        st.error(f"An unexpected error occurred during analysis: {e}")
        # Show detailed traceback in the app for easier debugging
        st.code(traceback.format_exc())
        return None

# --- Streamlit App Layout ---
st.set_page_config(layout="wide", page_title="Textract PDF Extractor")
st.title("📄 PDF Key-Value Extractor")
st.markdown(f"""
Upload a PDF document containing meeting notice information.
The application will use AWS Textract (in region **{AWS_REGION}**) to extract predefined fields.
""")

uploaded_file = st.file_uploader("Choose a PDF file", type="pdf", label_visibility="collapsed")

if uploaded_file is not None:
    st.info(f"Uploaded file: **{uploaded_file.name}** ({uploaded_file.size / 1024:.1f} KB)")

    if st.button("🔍 Analyze Document", type="primary"):
        # Read file bytes from the uploaded file object
        file_bytes = uploaded_file.getvalue()

        # Show spinner during processing
        with st.spinner(f"Analyzing document with AWS Textract... Please wait."):
            # Call the core processing function
            extracted_data_for_display = analyze_pdf_with_textract(
                file_bytes, AWS_REGION, TARGET_KEYS_MAP
            )

        # Display results if analysis was successful
        if extracted_data_for_display is not None:
            if not extracted_data_for_display:
                 st.warning("Analysis complete, but no relevant key-value pairs were found based on the configuration.")
            else:
                st.success("✅ Analysis complete!")
                st.subheader("Extracted Information:")

                # Prepare data for display using the ordered keys
                display_data = [{"Field": key, "Value": extracted_data_for_display.get(key, "Error: Key Missing")}
                                for key in ORDERED_DISPLAY_KEYS]

                # Convert to Pandas DataFrame for nice table display
                df = pd.DataFrame(display_data)

                # Display the table using st.dataframe
                st.dataframe(df, use_container_width=True, hide_index=True)

        # If extracted_data_for_display is None, an error message was already shown by the function

else:
    st.info("☝️ Please upload a PDF file to begin analysis.")

st.markdown("---")
st.caption("Powered by AWS Textract | Streamlit")
