from fastapi_celery.processors.helpers import xml_helper


def test_build_processor_setting_xml_basic():
    """Should return correct XML for valid processor arguments."""
    processor_args = [
        {"name": "a", "value": "1"},
        {"name": "b", "value": "2"},
        {"name": "c", "value": "3"},
    ]

    result = xml_helper.build_processor_setting_xml(processor_args)

    assert result is not None
    assert result.startswith("<PROCESSORSETTINGXML>")
    assert "<a>1</a>" in result
    assert "<b>2</b>" in result
    assert "<c>3</c>" in result
    assert result.endswith("</PROCESSORSETTINGXML>")


def test_build_processor_setting_xml_empty_list():
    """Should return None when given an empty list."""
    result = xml_helper.build_processor_setting_xml([])
    assert result is None


def test_build_processor_setting_xml_none_input():
    """Should return None when input is None."""
    result = xml_helper.build_processor_setting_xml(None)
    assert result is None


def test_build_processor_setting_xml_missing_name():
    """Should ignore entries missing 'name'."""
    processor_args = [
        {"name": "x", "value": "1"},
        {"value": "2"},  # missing name
    ]
    result = xml_helper.build_processor_setting_xml(processor_args)

    assert "<x>1</x>" in result
    assert "<value>2</value>" not in result


def test_build_processor_setting_xml_special_chars():
    """Should escape special XML characters correctly."""
    processor_args = [
        {"name": "msg", "value": 'Tom & Jerry <>"\''},
    ]

    result = xml_helper.build_processor_setting_xml(processor_args)

    assert "&amp;" in result
    assert "&lt;" in result
    assert "&gt;" in result
    assert "&quot;" in result
    assert "&apos;" in result
    assert "<msg>Tom &amp; Jerry &lt;&gt;&quot;&apos;</msg>" in result
