from models.class_models import StatusEnum, StepOutput


def submit(self, data_input, response_api, *args, **kwargs) -> StepOutput:

    return StepOutput(
            data=data_input.output,
            step_status=StatusEnum.SUCCESS,
            step_failure_message= None,
        )
