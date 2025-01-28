Troubleshooting
===============


### Error and Exception Investigation

Always begin by checking the exceptions trap table, which serves as the primary source for tracking system errors and exceptions.

### Job Status Issues

#### If job status is stuck in "Pending":

Check the event bus responses table for a corresponding entry
If no entry exists despite a successful API call:

Verify the event bus function's operational status
Review event bus logs for potential execution issues


#### If job status is stuck in "Running":

Locate the relevant function name in the event bus subscriptions table
Review the specific function's logs for execution problems or bottlenecks
