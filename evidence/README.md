# Trạng thái evidence

- **Học viên:** Nguyễn Hữu Nhật Minh
- **Mã học viên:** 2A202601551
- **Môi trường local:** `browser-fallback`; Docker full profile không chạy ổn định.

## Đã kiểm tra trên máy hiện tại

- Fast suite: 87 test đạt.
- Ruff, portability, integration matrix và Kubernetes/GitOps manifest validation: đạt.
- Kết quả được lưu trong `fast-suite.txt` và `preflight-local.json`.

## Evidence live chưa xác minh

- IP01: thiếu `ip01-kafka-consume.json` được sinh từ lần chạy hiện tại.
- IP04: thiếu `ip04-feast-online.json` được sinh từ lần chạy hiện tại.
- IP02, IP07, trace end-to-end, failure/recovery và GitOps live chưa được xác minh đầy đủ.

`integration-report.json` là artifact của lần chạy trước và không phản ánh việc IP01/IP04 đã bị loại khỏi bộ nộp hiện tại. Không dùng file này để khẳng định hai điểm đó đã đạt. Muốn xác minh lại cần chạy `integration-tests/test_j1_golden_path.py` trên máy đủ tài nguyên.
