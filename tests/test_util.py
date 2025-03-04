import re

from src.branch_storm.utils.formatters import error_formatter


def def_3(): raise ValueError("Now it is Value Error!")
def def_2(): return def_3()
def def_1(): return def_2()


def test_error_formatter_output_string():
    str_result = ""
    try:
        def_1()
    except Exception as exc:
        neg_message = "Negative message:"
        str_result = error_formatter(exc, neg_message, print_exc=False)

    actual_result = re.sub('File .*,', '', str_result)
    expected_result = '\nNegative message:\n   in test_error_formatter_output_string\n    def_1()\n   in def_1\n    ' \
                      'def def_1(): return def_2()\n   in def_2\n    def def_2(): return def_3()\n   in def_3\n    ' \
                      'def def_3(): raise ValueError("Now it is Value Error!")\nNow it is Value Error!'

    assert actual_result == expected_result


def test_error_formatter_stderr_output(capsys):
    try:
        def_1()
    except Exception as exc:
        neg_message = "Negative message:"
        error_formatter(exc, neg_message)

    captured = capsys.readouterr().err

    actual_result = re.sub('File .*,', '', captured)
    expected_result = '\nNegative message:\n   in test_error_formatter_stderr_output\n    def_1()\n   in def_1\n    ' \
                      'def def_1(): return def_2()\n   in def_2\n    def def_2(): return def_3()\n   in def_3\n    ' \
                      'def def_3(): raise ValueError("Now it is Value Error!")\nNow it is Value Error!\n'

    assert actual_result == expected_result
