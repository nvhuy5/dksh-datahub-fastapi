from models.class_models import GenericStepResult, StatusEnum, StepOutput


def publish_data(self, data_input, response_api, *args, **kwargs) -> StepOutput:
    # This method is intentionally left blank.
    # Subclasses or future implementations should override this to provide validation logic.
    return StepOutput(
        data=GenericStepResult(
            step_status="2",
            message="publish failed due to missing required fields."
        ),
        step_status=StatusEnum.FAILED,
        step_failure_message=["publish failed due to missing required fields."]
    )
