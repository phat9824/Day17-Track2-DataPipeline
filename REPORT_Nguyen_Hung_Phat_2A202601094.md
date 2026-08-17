# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Hùng Phát  
**MSSV:** 2A202601094  
**Lớp:** AICB-P2T2  
**Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

Kết quả cuối cùng: **4 / 4 tiêu chí đạt**.

<details>
<summary>Output <code>make verify</code> cuối cùng</summary>

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 18.1s
run 2/3 … 17.8s
run 3/3 … 17.8s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
  số file parquet                           ✗ 5,000 → 5,000
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Kết quả baseline trước khi sửa: `gold_training_set` 38.750 hàng, không ổn
định; `gold_feature_daily` 8.645/9.100; `quarantine_tickets` 0/312;
`silver_tickets.priority` có 6.606 giá trị sai hoặc `NULL`; `dbt test` 9/9
pass.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline, `gold_training_set` tăng số hàng dù nguồn không có thêm ticket mới. |
| **Nguyên nhân** | Model incremental chưa khai báo `unique_key` và chiến lược `merge`. Vì vậy dbt ghi thêm các hàng có `ticket_id` đã tồn tại thay vì cập nhật theo khoá; retry hoặc chạy lại pipeline tạo bản sao. |
| **Cách khắc phục** | Sửa `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` và `incremental_strategy='merge'`. Sửa `dags/ai_training_pipeline.py`: đặt `catchup=False`, `max_active_runs=1`. Giữ nguyên điều kiện lọc theo `run_date`. |
| **Bằng chứng trước khi sửa** | Sau lượt 1: **13.790** hàng. Sau lượt 2: **26.270** hàng. `gold_training_set` có 26.270 dòng nhưng chỉ **12.480** `ticket_id` khác nhau; trong khi `silver_tickets` có **12.480** dòng và **12.480** `ticket_id` khác nhau. |
| **Bằng chứng sau khi sửa** | `make verify` cuối: **12.480** hàng ở cả ba lượt; checksum `8dd7c98653` giống nhau ở 3 lượt; không có ticket lặp; DAG đạt `catchup=False`, `max_active_runs=1`. |

`gold_training_set` có grain là một **entity** (một ticket), với khoá tự nhiên
là `ticket_id`. Nguồn CDC có các bản ghi cập nhật (`op='u'`), nên cùng một
ticket có thể đi qua nhiều lần chạy/ngày; vì vậy cần merge theo khoá thay vì
append hoặc xoá/ghi lại theo partition ngày.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có 8.645 hàng, thiếu 455 hàng so với kỳ vọng 9.100; thiếu tập trung ở các ngày quá khứ. |
| **P99 độ trễ đo được** | **2,725833 ngày** (P50: 0,128090; P95: 1,813693; max: 2,944688; tỷ lệ trễ >1 ngày: 5,0509%). |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 2,725833 ngày để bao phủ 99% dữ liệu về muộn. |
| **Nguyên nhân** | Điều kiện incremental chỉ lấy `event_date` lớn hơn ngày lớn nhất đã có trong Gold. Event tới kho muộn có `event_date` thuộc ngày quá khứ nên không còn thoả điều kiện và bị bỏ sót vĩnh viễn. |
| **Cách khắc phục** | Sửa `dbt/models/gold/gold_feature_daily.sql`: dùng cửa sổ `event_date >= max(event_date) - interval 3 day`; thêm `unique_key=['event_date', 'customer_id']` và `incremental_strategy='merge'` để các cặp được tính lại bị cập nhật thay vì nhân bản. |
| **Bằng chứng** | trước: **8.645** hàng · sau: **9.100** hàng; checksum `3db448685c` giống nhau ở 3 lượt `make verify`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Dùng P99 giúp không nới lookback vĩnh viễn chỉ vì một outlier cực hiếm. Mỗi
> ngày lookback tăng thêm khiến pipeline phải tính lại thêm một ngày dữ liệu ở
> mọi lượt chạy. Trong bộ dữ liệu này max cũng dưới 3 ngày nên cả hai đều làm
> tròn thành 3 ngày; tuy nhiên P99 là chính sách bền vững hơn khi dữ liệu mới
> xuất hiện outlier lớn.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có **6.606** giá trị sai hoặc `NULL`; `quarantine_tickets` đang có 0 thay vì 312 hàng. |
| **Nguyên nhân** | Backend đổi biểu diễn từ số sang nhãn chữ từ 10/08. Macro chỉ dùng `try_cast`, nên biến nhãn hợp lệ thành `NULL` nhưng lại nhận các số ngoài miền 1..4; contract bị tắt và chưa có luồng quarantine. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `1`–`4`: giữ nguyên; (2) `urgent`/`high`/`medium`/`low`: map lần lượt sang 1/2/3/4; (3) `0`, `5`, `-1`, rỗng, `P1`, `P2`, `unknown`, `NULL`: trả `NULL` và đưa vào quarantine. |
| **Cách khắc phục** | Viết `CASE` trong `normalize_priority` để map nhãn hợp lệ và trả `NULL` cho dữ liệu lỗi. Trong `silver_tickets`, loại bản ghi có `priority_clean IS NULL` **trước** `row_number`; `quarantine_tickets` dùng cùng macro để lấy các bản ghi trả `NULL`. Bật `contract.enforced: true` và thêm test `not_null`, `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | Trước khi sửa: `quarantine_tickets` = 0; `dbt test` 9/9 pass; `silver_tickets.priority` sai/NULL = 6.606. Nhóm lỗi thật có **312** bản ghi: `0` (49), rỗng (43), `P1` (39), `unknown` (39), `P2` (38), `5` (37), `NULL` (35), `-1` (32). Sau khi sửa: quarantine = **312**, checksum `ebb89036fb` giống nhau ở 3 lượt; `priority` sạch; `dbt test` **11/11 pass**. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Giữ dữ liệu thô ở Bronze để còn audit, truy vết và có thể xử lý lại khi
> luật chuẩn hoá thay đổi. Kiểm tra và chuẩn hoá ở Silver; bản ghi lỗi được
> tách vào quarantine thay vì làm dừng pipeline, vì 312 bản ghi lỗi không nên
> chặn các dữ liệu hợp lệ còn lại phục vụ hệ thống.

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
