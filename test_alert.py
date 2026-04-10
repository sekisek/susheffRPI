from alerts import send_alert

result = send_alert(
    service="instagram",
    status="failure",
    reason="test_alert",
    message="This is a test alert from the Raspberry Pi.",
    screenshot_path=None,
    extra={"step": "3.3"}
)

print(result)
