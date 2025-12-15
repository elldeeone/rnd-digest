def handle_help() -> str:
    return (
        "Commands (control chat only):\n"
        "/help\n"
        "/health\n"
        "/latest [6h|2d]\n"
        "/search <terms>\n"
        "/ask [6h|2d|all] <question>\n"
        "/rollup <thread_id> [6h|2d|all|rebuild]\n"
        "/digest [6h|2d]  (no args = since last digest; duration is preview)\n"
        "/debug_ids\n"
        "/backfill_topics  (recover topic titles)\n"
        "/set_topic_title <thread_id> <title>\n"
    )
