#!/bin/sh
set -e

echo "=========================================="
echo " crypto_arb_feed"
echo " APP_TYPE: ${APP_TYPE:-feed}"
echo " JAVA_OPTS: ${JAVA_OPTS}"
echo "=========================================="

case "${APP_TYPE}" in
    feed)
        echo "[ENTRYPOINT] Starting Feed service..."
        exec java ${JAVA_OPTS} -cp "/app/lib/*" feed_filter.FeedFilterApp
        ;;
    feed-proxy)
        echo "[ENTRYPOINT] Starting Feed Proxy service..."
        exec java ${JAVA_OPTS} -cp "/app/lib/*" feed_proxy.FeedProxyApp
        ;;
    *)
        echo "[ENTRYPOINT] ERROR: Unknown APP_TYPE: ${APP_TYPE}"
        echo "[ENTRYPOINT] Valid options: feed, feed-proxy"
        exit 1
        ;;
esac
