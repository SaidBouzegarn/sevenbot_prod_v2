import streamlit as st
from pathlib import Path
import json
import logging
from datetime import datetime
import os
from utils.logging_config import setup_cloudwatch_logging

logger = setup_cloudwatch_logging('prompts_page')

# Cache agent and file listings
@st.cache_data(ttl=60)  # Cache for 1 minute
def get_all_agents():
    """Get all agent names from all levels"""
    try:
        logger.info("Fetching all agents")
        prompts_dir = Path(__file__).resolve().parent.parent / "Data" / "Prompts"
        agents = set()
        
        for level_dir in prompts_dir.glob("level*"):
            if level_dir.is_dir():
                for agent_dir in level_dir.iterdir():
                    if agent_dir.is_dir():
                        agents.add(agent_dir.name)
        
        return sorted(list(agents))
    except Exception as e:
        logger.error(f"Error getting agents: {e}", exc_info=True)
        return []

@st.cache_data(ttl=60)  # Cache for 1 minute
def get_agent_files(agent_name):
    """Get all .j2 and .json files for a specific agent across all levels"""
    try:
        prompts_dir = Path(__file__).resolve().parent.parent / "Data" / "Prompts"
        files = []
        
        for level_dir in prompts_dir.glob("level*"):
            agent_dir = level_dir / agent_name
            if agent_dir.exists():
                files.extend(list(agent_dir.glob("*.j2")))
                files.extend(list(agent_dir.glob("*.json")))
        
        return [(f, f"{f.parent.parent.name}/{f.parent.name}/{f.name}") for f in files]
    except Exception as e:
        logger.error(f"Error getting files for agent {agent_name}: {e}")
        return []

@st.cache_data
def read_file_content(file_path):
    """Read content from .j2 or .json file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if file_path.suffix == '.json':
                return json.dumps(json.loads(content), indent=2)
            return content
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return ""

def save_file_content(file_path, content):
    """Save content to file"""
    try:
        logger.info(f"Saving content to {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            if file_path.suffix == '.json':
                # Validate and format JSON before saving
                json_content = json.loads(content)
                f.write(json.dumps(json_content, indent=2))
            else:
                f.write(content)
        logger.info(f"Successfully saved content to {file_path}")
        return True
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format: {e}", exc_info=True)
        st.error("Invalid JSON format")
        return False
    except Exception as e:
        logger.error(f"Error saving file {file_path}: {e}", exc_info=True)
        st.error(f"Error saving file: {str(e)}")
        return False

def render_prompts_page():
    st.title("Prompts Manager")

    # Add custom CSS
    st.markdown("""
        <style>
        .stButton button {
            background: linear-gradient(120deg, #000000, #1e3a8a);
            color: white;
            border: none;
            padding: 0.6rem 1.2rem;
            border-radius: 5px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        .file-editor {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 5px;
            padding: 10px;
            margin-top: 1rem;
        }
        
        .info-box {
            padding: 1rem;
            background-color: rgba(255, 255, 255, 0.05);
            border-radius: 5px;
            margin-bottom: 1rem;
        }
        
        .stTextArea textarea {
            font-family: monospace;
        }
        </style>
    """, unsafe_allow_html=True)

    # Info box
    st.markdown("""
        <div class="info-box">
        Select an agent from the left panel to view and edit their prompt files.
        </div>
    """, unsafe_allow_html=True)

    # Create two columns: left for selection, right for editing
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Select Agent")
        agents = get_all_agents()
        
        if not agents:
            st.warning("No agents found")
            return
        
        selected_agent = st.selectbox(
            "Agent",
            options=agents,
            key="agent_selector"
        )

        if selected_agent:
            st.subheader("Select File")
            files = get_agent_files(selected_agent)
            
            if not files:
                st.warning(f"No files found for {selected_agent}")
                return
            
            # Display files with their relative paths
            selected_file = st.selectbox(
                "File",
                options=files,
                format_func=lambda x: x[1],  # Display the relative path
                key="file_selector"
            )

    with col2:
        if selected_file:
            st.subheader(f"Editing: {selected_file[1]}")
            
            # Read and display file content
            file_content = read_file_content(selected_file[0])
            
            # Create a text area for editing
            edited_content = st.text_area(
                "File Content",
                value=file_content,
                height=400,
                key="file_editor"
            )

            # Add save button without confirmation
            if st.button("Save Changes", help="Save modifications to the file"):
                if st.session_state.get("username_id"):  # Check if user is logged in
                    if save_file_content(selected_file[0], edited_content):
                        st.success("Changes saved successfully!")
                        # Log the change
                        logger.info(f"User {st.session_state.username_id} updated file {selected_file[1]}")
                    else:
                        st.error("Failed to save changes. Please check the format and try again.")
                else:
                    st.error("Please log in to make changes to the prompts.")

if __name__ == "__main__":
    render_prompts_page() 