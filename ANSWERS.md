# Báo cáo kỹ thuật Day 28 Track 2

## 1. Thông tin bài làm

- **Học viên:** Nguyễn Hữu Nhật Minh
- **Mã học viên:** 2A202601551
- **Hình thức:** Cá nhân
- **Phạm vi:** Hoàn thiện bốn hàm trong `src/lab28_platform/integration_tasks.py`, kiểm tra các ranh giới IP01–IP10 và tổng hợp evidence theo trạng thái thực tế của môi trường local.

## 2. Kiến trúc và trạng thái kiểm chứng

Luồng chính bắt đầu tại Envoy Gateway, đi vào API và Kafka, sau đó được xử lý bởi Airflow/Spark để ghi Delta Lake. Dữ liệu từ Delta được dùng cho Feast và Qdrant; phiên bản mô hình được quản lý bằng MLflow, còn vLLM đảm nhiệm suy luận khi có GPU endpoint. Prometheus, Grafana và tracing cung cấp tín hiệu quan sát xuyên suốt hệ thống.

| ID | Điểm kết nối | Cơ chế chính | Kết quả hiện có |
|---|---|---|---|
| IP01 | API → Kafka | `traceparent`, `idempotency-key`, topic `data.raw` | **UNVERIFIED:** evidence live đã được loại bỏ vì không thể tái tạo trên máy hiện tại |
| IP02 | Kafka → Airflow | DAG `lab28_ingestion_pipeline` | **UNVERIFIED:** máy local không đủ tài nguyên để chạy full profile |
| IP03 | Pipeline → Delta Lake | loại trùng trước khi ghi, transaction log | **Ready:** `documents` v0 có 1 dòng; `feedback` từ v0/2 dòng lên v1/3 dòng |
| IP04 | Delta → Feast | feature service `asker_serving_v1` | **UNVERIFIED:** evidence live đã được loại bỏ vì cần Airflow/Spark/Feast full profile |
| IP05 | Dữ liệu → Qdrant | hybrid retrieval, point ID tất định | **Ready:** collection có 13 point và truy vấn trả kết quả |
| IP06 | Đánh giá → MLflow | Model Registry và alias `champion` | **Ready:** `lab28-rag-release` version 2 là champion |
| IP07 | RAG → vLLM | API tương thích OpenAI và identity probe | **Not ready:** endpoint không kết nối được (`ConnectError`) |
| IP08 | Client → Envoy | routing và rate limit | **Có evidence riêng:** 13 request được nhận, 17 request bị giới hạn 429; integration report vẫn ghi unverified |
| IP09 | Services → Prometheus/Grafana | scrape metrics, dashboard, alert rule | **Có evidence riêng:** 9 target up, target vLLM optional down; integration report vẫn ghi unverified |
| IP10 | API → tracing backend | truyền W3C trace context | **Chưa đủ:** evidence chỉ có một span `GET` từ `lab28-api`, chưa chứng minh trace end-to-end |

`evidence/integration-report.json` được tạo từ lần chạy trước và ghi 5 điểm passing, 4 điểm unverified, IP07 not-ready, điểm số 83 và `ready=false`. Sau khi loại bỏ evidence IP01/IP04 không thể tái tạo, báo cáo JSON này chỉ còn giá trị tham khảo lịch sử, không đại diện cho trạng thái kiểm chứng hiện tại. Trong bộ evidence hiện tại, IP01 và IP04 phải được chấm là **UNVERIFIED** cho đến khi chạy lại J1 trên máy đủ tài nguyên.

## 3. Phần mã đã hoàn thiện

### 3.1. Header Kafka

`event_headers` luôn mã hóa `idempotency-key` thành `bytes`. Header `traceparent` chỉ được thêm khi đầu vào có giá trị, nhờ đó không phát tán một W3C trace header rỗng hoặc không hợp lệ.

### 3.2. Loại bản ghi trùng

`dedupe_latest` duyệt iterable đúng một lần và lưu một event cho từng `idempotency_key`. Khi một khóa xuất hiện nhiều lần, event có cặp `(occurred_at, event_id)` lớn hơn được giữ lại. Kết quả cuối được sắp xếp theo khóa để không phụ thuộc thứ tự Kafka giao bản tin.

### 3.3. Yêu cầu Feast online

`feast_online_request` dùng `FEATURE_REFS` từ contract chung thay vì chép lại danh sách feature. Payload gửi `entities={"asker_id": [asker_id]}` và đặt `full_feature_names=false`, đúng dạng Feast endpoint yêu cầu.

### 3.4. Trạng thái readiness

`readiness_status` ưu tiên lỗi bắt buộc: chỉ cần một probe `mandatory=true` không sẵn sàng thì kết quả là `not_ready`. Nếu các probe bắt buộc đều đạt nhưng có probe tùy chọn lỗi, kết quả là `degraded`; các trường hợp còn lại là `ready`.

## 4. Các đánh đổi kỹ thuật

### Idempotency thay cho ghi nối tiếp đơn thuần

Việc chọn event mới nhất theo khóa trước khi ghi Delta giúp replay không tạo nhiều bản ghi logic giống nhau. Đổi lại, hệ thống phải duy trì khóa ổn định và thực hiện so sánh/merge, tốn chi phí hơn append-only. Cách làm hiện tại còn dùng `event_id` để phá hòa khi hai event có cùng thời điểm, nên kết quả có tính tất định.

### Feast online store thay cho đọc trực tiếp Delta

Online store phù hợp với đường serving vì request được tạo theo contract cố định và lookup trả feature đã materialize. Chi phí của lựa chọn này là thêm một bước đồng bộ; feature có thể cũ hơn bản ghi Delta mới nhất. Evidence hiện tại ghi `freshness_seconds` khoảng 3612 giây và lookup khoảng 394 ms, vì vậy chưa thể khẳng định mức sub-10 ms.

### ID Qdrant tất định

Sinh point ID từ `doc_id` giúp lần index sau upsert đúng tài liệu thay vì nhân bản vector. Đổi lại, mọi producer phải dùng cùng namespace và thuật toán. Nếu thay embedding model, collection cũng cần chiến lược versioning hoặc re-index rõ ràng.

### Tách liveness, readiness và degraded

Liveness chỉ cho biết tiến trình còn phục vụ HTTP; readiness phản ánh khả năng đáp ứng dựa trên dependency. Dependency tùy chọn như vLLM có thể làm hệ thống degraded thay vì khiến toàn bộ API bị xem là chết. Cách này tăng khả năng phục vụ nhưng client và dashboard phải hiển thị rõ chất lượng suy giảm.

## 5. Khoảng cách trước khi dùng ở production

| Khoảng cách | Hiện trạng quan sát được | Hướng cải thiện |
|---|---|---|
| Kafka high availability | Evidence chỉ có 1 broker | Dùng cụm nhiều broker/AZ, replication và `min.insync.replicas` phù hợp |
| Readiness latency | 50 request có P50 2558 ms, P95 4296 ms, P99 10163 ms; 1 request lỗi | Chuyển probe dependency sang kiểm tra nền có cache và giới hạn timeout |
| GPU inference | vLLM không reachable | Cấp endpoint vLLM thật, xác minh `/version`, model và metric trước khi đánh dấu đạt |
| Trace continuity | Chỉ có 1 span từ API | Truyền và đối chiếu cùng trace ID qua gateway, Kafka, Airflow/Spark, retrieval và inference |
| Airflow/full stack | IP02 chưa chạy trên máy 3.76 GB RAM | Chạy full profile trên máy dùng chung đủ RAM và lưu DAG run/task output thật |
| Bảo mật | Các endpoint lab chủ yếu dùng HTTP nội bộ | Bổ sung TLS/mTLS, secret manager, phân quyền và rotation credential |
| Vận hành Delta | Evidence mới thể hiện các lần WRITE | Bổ sung retention, compaction, backup và diễn tập time-travel/restore |

## 6. Sự cố, phục hồi và giới hạn bằng chứng

Kịch bản phù hợp nhất với trạng thái hiện tại là mất endpoint vLLM. Kỳ vọng là liveness API vẫn hoạt động, probe vLLM báo lỗi và hệ thống chuyển sang degraded nếu vLLM được cấu hình là dependency tùy chọn. Sau khi endpoint phục hồi, cần xác minh identity, model được phục vụ và metric prefix của vLLM.

Repo hiện chỉ ghi nhận `ConnectError` cho IP07; chưa có chuỗi evidence trước–trong–sau phục hồi, Kafka offset và Delta row count tương ứng. Vì vậy phần no-data-loss của kịch bản này được ghi là **UNVERIFIED**, không được trình bày như kết quả đã chạy thành công.

## 7. Hiệu năng

`evidence/load-profile.json` ghi 50 request tới `/ready` với concurrency 4: 49 phản hồi HTTP 200 và 1 lỗi. P50 là 2558.21 ms, P95 là 4296.35 ms và P99 là 10163.38 ms. Nút thắt hợp lý nhất là các health check đồng bộ tới dependency, đặc biệt thời gian chờ kết nối vLLM và probe Kafka. Hướng xử lý là cache snapshot readiness trong thời gian ngắn, cập nhật bằng tác vụ nền và đặt timeout riêng cho từng dependency.

Các số liệu này chỉ phản ánh máy local và endpoint `/ready`; chúng không đại diện cho capacity production hoặc hiệu năng `/api/v1/ask`.

## 8. GitOps và rollback

Repo chứa Kubernetes manifests trong `deploy/kubernetes/base` và khai báo Argo CD trong `gitops/application.yaml`. Quy trình dự kiến gồm kiểm tra manifest, build image với tag bất biến, review Git diff rồi để Argo CD đồng bộ desired state. Rollback ứng dụng nên dùng `git revert` đối với commit cấu hình; rollback model dùng alias MLflow `champion` để trỏ lại version ổn định.

Chưa có artifact chứng minh một lần tạo drift, self-heal và rollback trên cluster thật. Mục này vì thế mô tả quy trình thiết kế; trạng thái thực thi là **UNVERIFIED** cho tới khi lưu được revision trước/sau cùng kết quả health check.

## 9. Phạm vi đóng góp cá nhân

Trong hình thức làm cá nhân, phạm vi chịu trách nhiệm bao phủ cả năm nhóm công việc:

1. Ingestion và orchestration: header Kafka, idempotency, topic và DAG evidence.
2. Data và ML: logic loại trùng, Delta history, Feast request và MLflow release.
3. Serving và retrieval: Qdrant retrieval, vLLM identity và degraded behavior.
4. Platform và observability: gateway rate limit, Prometheus/Grafana và trace continuity.
5. Trình bày: liên kết từng kết luận với file evidence, đồng thời công khai các mục `UNVERIFIED`.

Các file JSON trong `evidence/` là căn cứ cho số liệu trong báo cáo. Những nội dung chưa có artifact tương ứng được giữ ở trạng thái chưa xác minh thay vì suy diễn thành kết quả đã đạt.
