---
name: pulumi_infra
description: How to deploy DataRobot custom models and agentic workflows as Pulumi infrastructure in this repo. Use when adding or modifying resources in `deploy.py` or `infra/`, creating CustomModels, PredictionEnvironments, RegisteredModels, or Deployments with `pulumi_datarobot` and `datarobot_pulumi_utils`.
---

# Skill: DataRobot Pulumi Infrastructure

Working example: [infra/agent_infra.py](../../infra/agent_infra.py)

## Project layout

```
deploy.py               # Pulumi entry point (Pulumi.yaml: main: deploy.py)
infra/
  __init__.py           # Required — makes infra/ a package (avoids conflict with deploy.py name)
  agent_infra.py        # Component module(s)
  utils.py              # _generate_metadata_yaml helper
```

`deploy.py` adds `infra/` to `sys.path` so modules there can use bare imports:

```python
sys.path.append(str(Path(__file__).parent / "infra"))
from agent_infra import create_agent_infrastructure
```

## Key packages

```python
import pulumi_datarobot                                          # Pulumi resource types
import datarobot as dr                                          # dr.enums.*
from datarobot_pulumi_utils.schema.exec_envs import RuntimeEnvironments
from datarobot_pulumi_utils.schema.custom_models import DeploymentArgs, RegisteredModelArgs
from datarobot_pulumi_utils.pulumi.custom_model_deployment import CustomModelDeployment
```

## model-metadata.yaml (required before CustomModel)

DataRobot requires a `model-metadata.yaml` declaring runtime parameters **before** they can be set on the model. Generate it on the fly from the same `runtime_parameter_values` list:

```python
from utils import _generate_metadata_yaml

_generate_metadata_yaml(AGENT_NAME, str(AGENT_DIR), runtime_parameter_values)
```

- Writes `{AGENT_DIR}/model-metadata.yaml`
- Add `agent/model-metadata.yaml` to `.gitignore`
- Include the file in the `files` list sent to `CustomModel`

## Full deployment stack

Create resources in this order — each depends on the previous:

```python
# 1. Runtime params — type is always "string" for env-var-style config
runtime_parameter_values = [
    pulumi_datarobot.CustomModelRuntimeParameterValueArgs(key="model", type="string", value="vertex_ai/gemini-2.5-pro"),
    # ... one entry per config field, EXCLUDING datarobot_api_token and datarobot_endpoint (auto-injected)
]

# 2. Generate model-metadata.yaml from runtime_parameter_values
_generate_metadata_yaml(AGENT_NAME, str(AGENT_DIR), runtime_parameter_values)

# 3. UseCase — groups all assets in the DataRobot UI
use_case = pulumi_datarobot.UseCase(resource_name=NAME + " Use Case", name=NAME + " Use Case")

# 4. CustomModel — AgenticWorkflow type
agent_custom_model = pulumi_datarobot.CustomModel(
    resource_name="my-agent",
    name=NAME,
    base_environment_id=RuntimeEnvironments.PYTHON_311_GENAI_AGENTS.value.id,
    language="python",
    files=get_files(),          # list of (local_path, remote_name) tuples
    target_type="AgenticWorkflow",
    target_name="response",
    resource_bundle_id="cpu.3xlarge",
    replicas=1,
    runtime_parameter_values=runtime_parameter_values,
)

# 5. PredictionEnvironment
pred_env = pulumi_datarobot.PredictionEnvironment(
    resource_name=NAME + " Prediction Environment",
    name=NAME + " Prediction Environment",
    platform=dr.enums.PredictionEnvironmentPlatform.DATAROBOT_SERVERLESS,
    opts=pulumi.ResourceOptions(retain_on_delete=False),
)

# 6. CustomModelDeployment — wraps RegisteredModel + Deployment
deployment = CustomModelDeployment(
    resource_name=NAME + " Chat Deployment",
    use_case_ids=[use_case.id],
    custom_model_version_id=agent_custom_model.version_id,   # use version_id, not id
    prediction_environment=pred_env,
    registered_model_args=RegisteredModelArgs(
        resource_name=NAME + " Registered Model",
        name=NAME + " Registered Model",
    ),
    deployment_args=DeploymentArgs(
        resource_name=NAME + " Deployment",
        label=NAME + " Deployment",
        association_id_settings=pulumi_datarobot.DeploymentAssociationIdSettingsArgs(
            column_names=["association_id"],
            auto_generate_id=False,
            required_in_prediction_requests=True,
        ),
        predictions_data_collection_settings=pulumi_datarobot.DeploymentPredictionsDataCollectionSettingsArgs(
            enabled=True,
        ),
    ),
)
```

## File collection helper pattern

```python
def get_files() -> list[tuple[str, str]]:
    files = []
    for py_file in sorted((AGENT_DIR / "src").rglob("*.py")):
        files.append((str(py_file), py_file.name))
    files.append((str(AGENT_DIR / "requirements.txt"), "requirements.txt"))
    files.append((str(AGENT_DIR / "model-metadata.yaml"), "model-metadata.yaml"))
    return files
```

## Exports in deploy.py

```python
pulumi.export("agent_deployment_id", deployment.deployment_id)
pulumi.export("agent_registered_model_id", deployment.registered_model_id)
```

## Gotchas

- `CustomModelDeployment` requires **exactly one** of `custom_model_version_id` or `custom_model_args` — not both, not neither
- Use `agent_custom_model.version_id` (not `.id`) when passing to `CustomModelDeployment`
- `RuntimeEnvironments.PYTHON_311_GENAI_AGENTS.value.id` makes a live DataRobot API call — requires credentials at `pulumi up` time
- `infra/__init__.py` must exist or Python won't treat `infra/` as a package (it will try to import `infra.py` instead)
