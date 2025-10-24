from models.class_models import StatusEnum, StepOutput


def metadata_extract(self,input_data: StepOutput) -> StepOutput: # pragma: no cover  # NOSONAR

    return StepOutput(
            output=input_data.output,
            step_status=StatusEnum.SUCCESS,
            step_failure_message= None,
        )
