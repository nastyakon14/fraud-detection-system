SELECT transaction_id, score, fraud_flag, created_at
FROM transaction_scores
WHERE fraud_flag = 1
ORDER BY created_at DESC
LIMIT 10;