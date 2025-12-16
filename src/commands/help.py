def handle_help() -> str:
    return (
        "Commands (control chat only):\n"
        "/help\n"
        "/health\n"
        "/latest [6h|2d] [brief|full] [peek]  (no args = since last check-in)\n"
        "  (Shortcut: just send 'latest')\n"
        "/search <terms>\n"
        "/ask [6h|2d|all] <question>\n"
        "/topic <thread_id> [6h|2d|1w]\n"
        "/rollup <thread_id> [6h|2d|all|rebuild]\n"
        "/digest [6h|2d]  (no args = since last digest; duration is preview)\n"
        "/debug_ids\n"
        "/backfill_topics  (recover topic titles)\n"
        "/set_topic_title <thread_id> <title>\n"
    )
