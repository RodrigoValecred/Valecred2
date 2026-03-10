1. Add an email alert to `PL_Orquestracao_de_Dados_Incremental` pipeline that sends an email to `rodrigo@valecred.com.br` when any of the pipeline activities fail.
   - I have read `VALECRED_DEV/2_Pipelines/PL_Orquestracao_de_Dados_Incremental.DataPipeline/pipeline-content.json`.
   - I appended a new activity of type `Office365Outlook` to send an email to the specified address.
   - It depends on all previous activities, and its dependency condition is "Failed".
2. Pre commit steps.
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
3. Submit the change.
