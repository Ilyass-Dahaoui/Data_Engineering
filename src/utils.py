from datetime import datetime
from typing import List, Dict, Any


def merge_reviews(existing: List[Dict[str, Any]], new: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge two lists of review dicts using review_id as key.
    If a review_id exists in both, the newer dictionary overwrites.
    """
    merged = {r['review_id']: r for r in existing}
    for r in new:
        rid = r.get('review_id')
        if rid is None:
            continue
        merged[rid] = r
    return list(merged.values())


def scd2_update(existing: List[Dict[str, Any]],
                incoming: List[Dict[str, Any]],
                key: str = 'app_id',
                timestamp: str = None) -> List[Dict[str, Any]]:
    """Perform a simple SCD2 upsert on a list of existing historical records.

    Args:
        existing: history table records containing fields:
            key, all attributes, start_date, end_date, current_flag
        incoming: new snapshot of dimension table (no scd metadata)
        key: name of primary key field to compare
        timestamp: ISO string for current processing time; if None, now() is used

    Returns:
        updated history list with new records appended and old ones closed
    """
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat()

    # build maps for current active rows
    current = {row[key]: row for row in existing if row.get('current_flag', False)}
    out_history = existing.copy()

    incoming_map = {row[key]: row for row in incoming}

    # close records that no longer exist
    for pk, row in list(current.items()):
        if pk not in incoming_map:
            # mark end_date and deactivate
            if row.get('end_date') is None:
                row['end_date'] = timestamp
                row['current_flag'] = False

    # process incoming rows
    for pk, new_row in incoming_map.items():
        if pk not in current:
            # new record entirely; insert as current
            rec = new_row.copy()
            rec['start_date'] = timestamp
            rec['end_date'] = None
            rec['current_flag'] = True
            out_history.append(rec)
        else:
            old = current[pk]
            # compare attributes aside from scd metadata
            # drop scd columns to compare
            old_attrs = {k: v for k, v in old.items()
                         if k not in ('start_date', 'end_date', 'current_flag')}
            if any(old_attrs.get(k) != new_row.get(k) for k in new_row):
                # close old
                old['end_date'] = timestamp
                old['current_flag'] = False
                # add new
                rec = new_row.copy()
                rec['start_date'] = timestamp
                rec['end_date'] = None
                rec['current_flag'] = True
                out_history.append(rec)
    return out_history
