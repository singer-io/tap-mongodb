#!/usr/bin/env python3

import singer

def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bk in [non_whitelisted_bookmark_key
               for non_whitelisted_bookmark_key
               in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
               if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bk)
