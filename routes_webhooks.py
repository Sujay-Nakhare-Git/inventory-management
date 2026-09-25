from core import *  # noqa: F401,F403


def _valid_whatsapp_signature(raw_body, signature, app_secret):
    if not signature or not signature.startswith("sha256=") or not app_secret:
        return False
    expected = hmac.new(
        app_secret.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature[7:], expected)


def _store_whatsapp_event(db, event):
    db.execute(
        "INSERT OR IGNORE INTO whatsapp_webhook_events "
        "(event_key, event_type, message_id, customer_phone, message_type, "
        "message_text, status, event_timestamp, payload_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event["event_key"],
            event["event_type"],
            event.get("message_id"),
            event.get("customer_phone"),
            event.get("message_type"),
            event.get("message_text"),
            event.get("status"),
            event.get("event_timestamp"),
            json.dumps(event["payload"], ensure_ascii=True, separators=(",", ":")),
        ),
    )


def _message_text(message):
    message_type = message.get("type", "")
    if message_type == "text":
        return str(message.get("text", {}).get("body", ""))
    if message_type == "button":
        return str(message.get("button", {}).get("text", ""))
    if message_type == "interactive":
        interactive = message.get("interactive", {})
        reply = interactive.get("button_reply") or interactive.get("list_reply") or {}
        return str(reply.get("title", ""))
    return ""


def _whatsapp_events(payload):
    for entry in payload.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value", {})
            for message in value.get("messages", []):
                message_id = str(message.get("id", "")).strip()
                if not message_id:
                    continue
                yield {
                    "event_key": f"message:{message_id}",
                    "event_type": "incoming_message",
                    "message_id": message_id,
                    "customer_phone": str(message.get("from", "")),
                    "message_type": str(message.get("type", "")),
                    "message_text": _message_text(message),
                    "event_timestamp": str(message.get("timestamp", "")),
                    "payload": message,
                }

            for status in value.get("statuses", []):
                message_id = str(status.get("id", "")).strip()
                status_name = str(status.get("status", "")).strip()
                timestamp = str(status.get("timestamp", "")).strip()
                if not message_id or not status_name:
                    continue
                yield {
                    "event_key": f"status:{message_id}:{status_name}:{timestamp}",
                    "event_type": "status_update",
                    "message_id": message_id,
                    "customer_phone": str(status.get("recipient_id", "")),
                    "status": status_name,
                    "event_timestamp": timestamp,
                    "payload": status,
                }


@app.route("/webhooks/whatsapp", methods=["GET", "POST"])
def whatsapp_webhook():
    verify_token, app_secret = load_whatsapp_webhook_config()

    if request.method == "GET":
        mode = request.args.get("hub.mode", "")
        token = request.args.get("hub.verify_token", "")
        challenge = request.args.get("hub.challenge", "")
        if (
            mode == "subscribe"
            and verify_token
            and hmac.compare_digest(token, verify_token)
        ):
            return Response(challenge, status=200, mimetype="text/plain")
        return jsonify({"error": "Webhook verification failed."}), 403

    raw_body = request.get_data(cache=True)
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not _valid_whatsapp_signature(raw_body, signature, app_secret):
        return jsonify({"error": "Invalid webhook signature."}), 403

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or payload.get("object") != "whatsapp_business_account":
        return jsonify({"error": "Invalid webhook payload."}), 400

    db = get_db()
    event_count = 0
    for event in _whatsapp_events(payload):
        _store_whatsapp_event(db, event)
        event_count += 1
    db.commit()

    return jsonify({"received": True, "events": event_count})