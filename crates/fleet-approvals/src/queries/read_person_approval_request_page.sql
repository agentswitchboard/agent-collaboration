SELECT requests.*
FROM approval_requests AS requests
WHERE requests.organization_id = ?1
  AND (?2 = '' OR requests.request_id < ?2)
  AND (?3 = 1 OR requests.record_state = 'waiting')
  AND (requests.record_state <> 'ended' OR requests.erase_after_ms IS NULL OR requests.erase_after_ms > ?4)
  AND EXISTS (
      SELECT 1 FROM approval_request_destinations AS destination
      WHERE destination.approval_row_id = requests.approval_row_id
        AND destination.organization_id = requests.organization_id
        AND destination.destination_kind = 'people'
  )
ORDER BY requests.request_id DESC
LIMIT ?5
