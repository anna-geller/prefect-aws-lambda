from prefect.blocks.system import JSON

json = JSON(value=dict(threshold=42))
json.save(name="max-value", overwrite=True)
