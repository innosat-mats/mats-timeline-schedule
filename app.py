#!/usr/bin/env python3

from aws_cdk import App

from stacks.mats_schedule_stack import MatsScheduleStack

app = App()

MatsScheduleStack(
    app,
    "MatsScheduleToParquetStack",
    input_bucket_name="ops-mats-schedule-source-v0.1",
    output_bucket_name="ops-mats-schedule-v0.1",
    queue_arn_export_name="L0ScheduleFetcherStackOutputQueue",
)

app.synth()
