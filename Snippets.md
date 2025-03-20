### Auto Bootstrap the CDK Environment Prior to Deployment
```bash
poetry run cdk bootstrap aws://$(aws sts get-caller-identity --query Account --output text)/us-west-2
```

This is necessary to create the CDK environment in your AWS account before deploying any stacks. 
Ensure that the region is correct for your deployment prior to running this command.

### Deploy all stacks without prompts (CDK Method)
```bash
poetry run cdk deploy --all --force --require-approval never
````

### Delete all stacks without prompts (CDK Method)
```bash
# Detach all IAM policies from roles prefixed with omnilake-dev. These would prevent the removal of the stack.
for role in $(aws iam list-roles --query 'Roles[?starts_with(RoleName, `omnilake-dev`)].RoleName' --output text); do
  for policy_arn in $(aws iam list-attached-role-policies --role-name $role --query 'AttachedPolicies[].PolicyArn' --output text); do
    aws iam detach-role-policy --role-name $role --policy-arn $policy_arn
  done
done

# Remove all CDK deployed stacks without prompting
cdk destroy --all --force --require-approval never
```

### Delete all stacks with a specific prefix. Brute force method.
```bash
#!/bin/bash

stacks=$(aws cloudformation list-stacks \
    --stack-status-filter CREATE_COMPLETE DELETE_FAILED UPDATE_COMPLETE ROLLBACK_COMPLETE \
    --query 'StackSummaries[?starts_with(StackName, `omnilake`) == `true`].StackName' \
    --output text | tr '\t' '\n')

if [ -z "$stacks" ]; then
    echo "No matching stacks found. Exiting."
    exit 0
fi

echo "Deleting stacks:"
echo "$stacks"

while IFS= read -r stack; do
    if [[ -n \"$stack\" ]]; then
        echo "Deleting stack: $stack"
        aws cloudformation delete-stack --stack-name "$stack"
    fi
done <<< "$stacks"
```
You can use this command to effectively remove Omnilake from your AWS account.

### Standard Demo Query
```bash
poetry run omni question "What is omnilake?"
```

### Index the current directory
```bash
poetry run omni --base_dir "$(pwd)" index
```

### List IAM roles prefixed with omnilake-dev
```bash
aws iam list-roles --query 'Roles[?starts_with(RoleName, `omnilake-dev`)].RoleName' --output text
```

### Update DynamoDB items (composite key) from IN_PROGRESS to FAILED
```bash
aws dynamodb scan \
  --table-name omnilake-dev-jobs \
  --filter-expression "#S = :inprogress" \
  --expression-attribute-names '{"#S":"Status"}' \
  --expression-attribute-values '{":inprogress":{"S":"IN_PROGRESS"}}' \
  --query "Items[].{JobType:JobType.S, JobId:JobId.S}" \
  --output json | jq -c '.[]' | while read item; do
    job_type=$(echo $item | jq -r '.JobType')
    job_id=$(echo $item | jq -r '.JobId')
    aws dynamodb update-item \
      --table-name omnilake-dev-jobs \
      --key "{\"JobType\":{\"S\":\"$job_type\"},\"JobId\":{\"S\":\"$job_id\"}}" \
      --update-expression "SET #S = :failed" \
      --expression-attribute-names '{"#S":"Status"}' \
      --expression-attribute-values '{":failed":{"S":"FAILED"}}';
done
```

This is useful for halting jobs that are stuck in the IN_PROGRESS state and preventing recurring executions.

### Tail All Log Groups
```bash
log_groups=(
  "/aws/lambda/omnilake-dev-aistatistics-AWS679f53fac002430cb0da5-RzXXG39Oqg1h"
  "/aws/lambda/omnilake-dev-archive-vector-data-retrieval"
  "/aws/lambda/omnilake-dev-archive-vector-indexer"
  "/aws/lambda/omnilake-dev-archive-vector-provisioner"
  "/aws/lambda/omnilake-dev-corestack-AWS679f53fac002430cb0da5b79-t8loqiCRUJZL"
  "/aws/lambda/omnilake-dev-event_bus-async-handler"
  "/aws/lambda/omnilake-dev-event_bus_responses-rest-handler"
  "/aws/lambda/omnilake-dev-eventbusstac-AWS679f53fac002430cb0da5-snQ8j2TwxUeD"
  "/aws/lambda/omnilake-dev-exceptions_trap-rest-handler"
  "/aws/lambda/omnilake-dev-lake-request-init"
  "/aws/lambda/omnilake-dev-lake-request-chain-mgr"
  "/aws/lambda/omnilake-dev-new-entry-processor"
  "/aws/lambda/omnilake-dev-lake-request-lookup-coordination"
  "/aws/lambda/omnilake-dev-omnilake-private-api-rest-handler"
)

for log_group in "${log_groups[@]}"; do
  aws logs tail "$log_group" --follow --since 10m --format short &
done

wait
```