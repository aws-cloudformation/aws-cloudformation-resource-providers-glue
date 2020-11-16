# AWS::Glue::SchemaVersion

Congratulations on starting development! Next steps:

1. For updating the resource contract update `aws-glue-schemaversion.json` and run `cfn generate`.
1. Modify the appropriate handler.
1. Run `mvn install`
1. Create test files as mentioned [here](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-walkthrough.html#resource-type-walkthrough-test).
1. Install [SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html).
1. Run the command, `sam local invoke TestEntrypoint --event sam-tests/<create.json>` to test the respective handler.

Read [CFN Resource development guide](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-walkthrough.html) for more guidance.

> Please don't modify files under `target/generated-sources/rpdk`, as they will be automatically overwritten.

The code uses [Lombok](https://projectlombok.org/), and [you may have to install IDE integrations](https://projectlombok.org/setup/overview) to enable auto-complete for Lombok-annotated classes.
