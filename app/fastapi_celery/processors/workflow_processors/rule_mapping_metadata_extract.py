from models.class_models import StatusEnum, StepOutput


def metadata_extract(self, data_input, response_api, *args, **kwargs) -> StepOutput:

    return StepOutput(
            data=data_input.output,
            step_status=StatusEnum.SUCCESS,
            step_failure_message= None,
        )
