def handle_help() -> str:
    return (
        "Commands (control chat only):\n"
        "/help\n"
        "/health\n"
        "/latest [6h|2d]\n"
        "/search <terms>\n"
        "/digest  (manual digest since last run)\n"
        "/debug_ids\n"
        "/backfill_topics  (recover topic titles)\n"
        "/set_topic_title <thread_id> <title>\n"
    )
