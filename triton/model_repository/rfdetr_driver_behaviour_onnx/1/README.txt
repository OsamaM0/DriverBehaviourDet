# Place the ONNX file as `model.onnx` in this directory:
#   ln -sf ../../../../rf_detr_driver_behaviour_optimized.onnx \
#          triton/model_repository/rfdetr_driver_behaviour_onnx/1/model.onnx
#
# The repository version directory MUST be named `1` (or a higher integer for
# subsequent versions). Triton will hot-load on file change when run with
# `--model-control-mode=poll --repository-poll-secs=10`.
