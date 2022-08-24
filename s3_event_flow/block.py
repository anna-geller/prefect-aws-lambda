from prefect.blocks.system import String

block = String(value="42")
block.save(name="max-value", overwrite=True)
