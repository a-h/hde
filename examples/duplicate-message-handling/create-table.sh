aws dynamodb create-table \
  --table-name duplicate-message-handling \
  --attribute-definitions AttributeName=_id,AttributeType=S AttributeName=_rng,AttributeType=S \
  --key-schema AttributeName=_id,KeyType=HASH AttributeName=_rng,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
