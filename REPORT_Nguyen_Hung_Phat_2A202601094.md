# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Hùng Phát  
**MSSV:** 2A202601094  
**Lớp:** AICB-P2T2  
**Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

Kết quả ban đầu (trước khi khắc phục): **1 / 4 tiêu chí đạt**.

| Bảng / kiểm tra | Kết quả ban đầu |
|---|---:|
| `gold_training_set` | 38.750 hàng; không ổn định; kỳ vọng 12.480 |
| `gold_feature_daily` | 8.645 hàng; kỳ vọng 9.100 |
| `gold_doc_chunks` | 31.200 hàng; ổn định (đúng) |
| `quarantine_tickets` | 0 hàng; kỳ vọng 312 |
| `silver_tickets.priority` sai / NULL | 6.606 hàng |
| `dbt test` | 9/9 pass |

> **Cần làm trước khi nộp:** thay phần này bằng nguyên output `make verify`
> cuối cùng, sau khi hoàn thành ba nhiệm vụ.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline, `gold_training_set` tăng số hàng dù nguồn không có thêm ticket mới. |
| **Nguyên nhân** | Model incremental chưa khai báo `unique_key` và chiến lược `merge`. Vì vậy dbt ghi thêm các hàng có `ticket_id` đã tồn tại thay vì cập nhật theo khoá; retry hoặc chạy lại pipeline tạo bản sao. |
| **Cách khắc phục** | Sửa `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` và `incremental_strategy='merge'`. Sửa `dags/ai_training_pipeline.py`: đặt `catchup=False`, `max_active_runs=1`. Giữ nguyên điều kiện lọc theo `run_date`. |
| **Bằng chứng trước khi sửa** | Sau lượt 1: **13.790** hàng. Sau lượt 2: **26.270** hàng. `gold_training_set` có 26.270 dòng nhưng chỉ **12.480** `ticket_id` khác nhau; trong khi `silver_tickets` có **12.480** dòng và **12.480** `ticket_id` khác nhau. |
| **Bằng chứng sau khi sửa** | `make verify`: **12.480** hàng ở cả ba lượt; checksum `8622572a97` giống nhau ở 3 lượt; không có ticket lặp; DAG đạt `catchup=False`, `max_active_runs=1`. |

`gold_training_set` có grain là một **entity** (một ticket), với khoá tự nhiên
là `ticket_id`. Nguồn CDC có các bản ghi cập nhật (`op='u'`), nên cùng một
ticket có thể đi qua nhiều lần chạy/ngày; vì vậy cần merge theo khoá thay vì
append hoặc xoá/ghi lại theo partition ngày.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có 8.645 hàng, thiếu 455 hàng so với kỳ vọng 9.100; thiếu tập trung ở các ngày quá khứ. |
| **P99 độ trễ đo được** | **… ngày** *(bắt buộc — sẽ đo trước khi sửa)* |
| **Lookback đã chọn** | … ngày — vì … |
| **Nguyên nhân** | *Chờ xác nhận bằng phép đo P99 và đọc điều kiện incremental.* |
| **Cách khắc phục** | *Chờ thực hiện.* |
| **Bằng chứng** | trước: **8.645** hàng · sau: … hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> *Chờ trả lời sau khi đo phân bố độ trễ của `bronze_events`.*

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có **6.606** giá trị sai hoặc `NULL`; `quarantine_tickets` đang có 0 thay vì 312 hàng. |
| **Nguyên nhân** | *Chờ điều tra phân bố `priority_raw` và cấu hình contract.* |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | *Chờ thực hiện.* |
| **Cách khắc phục** | *Chờ thực hiện.* |
| **Bằng chứng** | `quarantine_tickets` = 0 hàng trước khi sửa · `dbt test` 9/9 pass trước khi sửa |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> *Chờ trả lời sau khi hoàn thành nhiệm vụ 3.*

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Chưa làm |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain, natural key, chiến lược incremental và hành vi khi retry. |
| 2 | Đo độ trễ giữa thời điểm event xảy ra và thời điểm dữ liệu tới kho trước khi chọn lookback window. |
| 3 | Kiểm tra contract, schema evolution và cách cô lập bản ghi lỗi để pipeline vẫn phục vụ được dữ liệu hợp lệ. |
