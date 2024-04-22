{{config (
    materialized = 'incremental',
    unique_key = '_pk'
)}}


WITH RECURSIVE SeatNumbers AS (
    SELECT 0 AS seat_num
    UNION ALL
    SELECT seat_num + 1
    FROM SeatNumbers
    WHERE seat_num < (SELECT MAX(num_seats) - 1 FROM stg_tm_ticket_exchange)
),
seat_num_ownership_dummy AS (
    SELECT
  t.ticket_exchange_id,
        t.event_id,
        t.account_id,
        t.seat_section_id,
        t.seat_row_num,
        t.first_seat_num + sn.seat_num AS seat_num,
        t.activity_date,
        t.rep_email,
        t.total_ticket_price,
        t.order_num
    FROM
        stg_tm_ticket_exchange t
    JOIN
        SeatNumbers sn ON sn.seat_num < t.num_seats
),
seat_num_ownership AS (
    SELECT *
    FROM seat_num_ownership_dummy
    ORDER BY
        account_id,
        seat_section_id,
        seat_row_num,
        seat_num
)
SELECT
CONCAT(s.event_id, '_', s.seat_section_id, '_', s.seat_row_num, '_', s.seat_num) AS _pk,
    s.event_id,
    e.event_name,
    e.season_id,
    e.event_date_start,
    s.ticket_exchange_id,
    s.account_id,
    c.contact_id,
    c.first_name,
    c.last_name,
    c.email,
    c.mailing_address,
    c.mailing_city,
    c.mailing_state,
    c.mailing_zip,
    s.seat_section_id,
    s.seat_row_num,
    s.seat_num,
    s.activity_date,
    CASE WHEN s.rep_email IS NOT NULL THEN true ELSE false END AS is_original,
    CASE WHEN s.rep_email IS NULL AND s.total_ticket_price IS NOT NULL THEN true ELSE false END AS is_resale,
    CASE WHEN s.rep_email IS NULL AND s.total_ticket_price IS NULL THEN true ELSE false END AS is_transfer,
    s.rep_email,
    s.total_ticket_price,
    s.order_num
FROM
    seat_num_ownership AS s
LEFT JOIN
    stg_tm_event AS e ON s.event_id = e.event_id
LEFT JOIN
    stg_crm_contact AS c ON s.account_id = c.tm_account_id
WHERE
    NOT EXISTS (
        SELECT 1
        FROM seat_num_ownership AS t2
        WHERE
            s.event_id = t2.event_id
            AND s.seat_section_id = t2.seat_section_id
            AND s.seat_row_num = t2.seat_row_num
            AND s.seat_num = t2.seat_num
            AND s.account_id != t2.account_id
            AND s.activity_date < t2.activity_date
    )