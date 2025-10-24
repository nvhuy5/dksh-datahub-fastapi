from typing import List, Dict
from utils import log_helpers


# === Set up logging ===
logger = log_helpers.get_logger("xml_helper")

def build_processor_setting_xml(processor_args: List[Dict[str, str]]) -> str | None:
    """
    Convert processorArgumentDtos into XML format like:
    <PROCESSORSETTINGXML>
      <param_name>param_value</param_name>
      ...
    </PROCESSORSETTINGXML>

    Args:
        processor_args (List[Dict[str, str]]): List of processorArgumentDtos containing 'processorArgumentName' and 'value'.

    Returns:
        str | None: XML string or None if list is empty or invalid.
    """
    if not processor_args:
        logger.warning("[build_processor_setting_xml] No processor arguments provided.")
        return None

    xml_lines = ["<PROCESSORSETTINGXML>"]
    for arg in processor_args:
        name = arg.get("name")
        value = arg.get("value", "")
        if name:
            # escape special XML chars
            safe_value = (
                str(value)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;")
            )
            xml_lines.append(f"  <{name}>{safe_value}</{name}>")
        else:
            logger.warning(f"[build_processor_setting_xml] Missing processorArgumentName in {arg}")

    xml_lines.append("</PROCESSORSETTINGXML>")
    xml_string = "\n".join(xml_lines)

    logger.info(f"[build_processor_setting_xml] Generated XML:\n{xml_string}")
    return xml_string
