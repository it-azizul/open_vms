import pyodbc
import frappe
import datetime
from datetime import timedelta, date
from frappe.utils import getdate, nowdate


def on_leave_submit(doc, method):
    
    try:
        # call the instance method you wrote in LeaveApplication
        sync_to_mssql(doc)

    except Exception as e:
        frappe.log_error(f"on_leave_submit failed: {e}", "open_vms.mssql_sync.on_leave_submit")

def on_leave_cancel(doc, method):
    print("----- on_leave_cancel called -----")
    try:
        # call the cancel helpers you already created
        delete_emp_attendance(doc)
        delete_empleave_on_cancel(doc)
    except Exception as e:
        frappe.log_error(f"on_leave_cancel failed: {e}", "open_vms.mssql_sync.on_leave_cancel")


def sync_to_mssql(doc):
    """
    Call only this method from after_submit:
        doc.sync_to_mssql()
    """

    conn = None
    cur = None

    try:
        conn = mssql_connect(doc)
        if not conn:
            frappe.log_error("Could not connect to MSSQL server", "LeaveApplication.sync_to_mssql")
            return

        cur = conn.cursor()

        emp_code = get_employee_code(doc)
        leave_short = get_leave_short_code(doc)
        fd, td, leave_days = get_date_range(doc)
        # Insert into EmpLeaveTransactionApproved
        txn_id = insert_leave_transaction(
            doc, cur, emp_code, leave_short, fd, td, leave_days
        )

        # Shift details
        shift_id, shift_in, shift_out = get_shift_info(doc, cur, emp_code)
        print(f"--- Shift Info: ID={shift_id}, In={shift_in}, Out={shift_out} ---")
        # Attendance Update/Insert
        process_attendance(
            doc,
            cur, emp_code, leave_short,
            shift_id, shift_in, shift_out,
            fd, td
        )
        conn.commit()
        # frappe.logger().info(f"Leave synced. TXN={txn_id}, Emp={emp_code}")

    except Exception as e:
        if conn:
            conn.rollback()
        frappe.log_error(f"MSSQL Sync Failed: {e}", "LeaveApplication.sync_to_mssql")

    finally:
        mssql_close(doc, cur, conn)


def mssql_connect(doc):
    # Change the configuration as per your MSSQL setup
    cfg = {
        "server": "192.168.166.80",
        "port": "1433",
        "database": "HRMS5",
        "username": "azizul",
        "password": "azizul123",
        "driver": "ODBC Driver 18 for SQL Server",
    }

    conn_str = (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']},{cfg['port']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};PWD={cfg['password']};"
        f"TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str, timeout=5)


def mssql_close(doc, cur, conn):
    try:
        if cur: cur.close()
    except: pass
    try:
        if conn: conn.close()
    except: pass


def get_employee_code(doc):
    try:
        return frappe.db.get_value(
            "Employee", doc.employee, "employee_number"
        ) or str(doc.employee)
    except:
        return str(doc.employee)


def get_leave_short_code(doc):
    map = {
        "Casual Leave": "CL",
        "Sick Leave": "SL",
        "Leave Without Pay": "LWP",
    }
    raw = (doc.leave_type or "").strip()
    return map.get(raw, raw.upper())


def get_date_range(doc):
    from frappe.utils import getdate, nowdate

    fd = getdate(doc.from_date or nowdate())
    td = getdate(doc.to_date or fd)
    leave_days = (td - fd).days + 1
    return fd, td, leave_days


def generate_txn_id(doc, cur):
    import datetime
    year_suffix = datetime.date.today().strftime("%y")
    pattern = f"LVTR-{year_suffix}-%"

    cur.execute("""
        SELECT MAX(
            TRY_CAST(
                RIGHT(TransactionID, CHARINDEX('-', REVERSE(TransactionID)) - 1) AS INT
            )
        )
        FROM EmpLeaveTransactionApproved
        WHERE TransactionID LIKE ?
    """, (pattern,))

    row = cur.fetchone()
    last = row[0] if row and row[0] else 0
    return f"LVTR-{year_suffix}-{str(last+1).zfill(5)}"


def insert_leave_transaction(doc, cur, emp_code, leave_short, fd, td, leave_days, leave_reason=None):
    """
    Super-short clean version.
    All defaults handled directly inside SQL.
    Caller will do commit/rollback.
    """
    try:
        txn_id = generate_txn_id(doc, cur)

        # leave reason fallback
        lr = leave_reason or getattr(doc, "reason", "") or getattr(doc, "leave_reason", "") or ""

        # fetch LeavePolicyID directly
        cur.execute("SELECT LeavePolicyID FROM LeavePolicyMaster WHERE LeaveType = ?", (leave_short,))
        row = cur.fetchone()
        leave_policy_id = row[0] if row else None

        cur.execute("""
            INSERT INTO EmpLeaveTransactionApproved
            (TransactionID, EmployeeCode, TransactionDate, LeavePolicyID,
            FromDate, ToDate, LeaveDays, LeaveDaysNextYr, ReqSync, LeaveReason,
            LeaveAvailPlace, IsForwarded, IsRecomended, IsApproved, IsRejected,
            AddedBy, DateAdded, UpdatedBy, UpdatedDate,
            IsPostApproved, IsPreApproved, NeedAttendanceDelete)
            VALUES (
                ?, ?, GETDATE(), ?, ?, ?, ?, 
                0,              -- LeaveDaysNextYr
                1,              -- ReqSync
                ?,              -- LeaveReason
                '',             -- LeaveAvailPlace
                1, 1, 1, 0,     -- Flags: Forwarded, Recommended, Approved, Rejected
                ?, GETDATE(),   -- AddedBy, DateAdded
                ?, GETDATE(),   -- UpdatedBy, UpdatedDate
                1,              -- IsPostApproved
                0,              -- IsPreApproved
                0               -- NeedAttendanceDelete
            )
        """, (
            txn_id,
            emp_code,
            leave_policy_id,
            fd,
            td,
            float(leave_days),
            lr,
            frappe.session.user,
            frappe.session.user,
        ))

        return txn_id

    except Exception as e:
        frappe.log_error(f"Insert Leave TXN failed: {e}", "insert_leave_transaction")


def get_shift_info(doc, cur, emp_code):
    cur.execute("""
        SELECT TOP 1 SP.ShiftID, SP.InTime, SP.OutTime
        FROM ShiftPlan SP
        LEFT JOIN EmployeePIMSInfo EI ON EI.ShiftID = SP.ShiftID
        WHERE EI.EmployeeCode = ?
    """, (emp_code,))

    row = cur.fetchone()
    print("--- Shift Info Row:", row, "---")
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def process_attendance(doc, cur, emp_code, leave_short,
                        shift_id, shift_in, shift_out,
                        fd, td):

    from datetime import timedelta

    cur_date = fd
    added_by = frappe.session.user

    while cur_date <= td:

        # check existing attendance
        cur.execute("""
            SELECT TOP 1 DayStatus
            FROM DayWisePayHour
            WHERE EmployeeCode=? AND WorkDate=? AND IsDefault=1
        """, (emp_code, cur_date))

        row = cur.fetchone()

        if row:

            print("--------------------------")
            print(cur_date)
            # update if absent
            if row[0] == "A":
                cur.execute("""
                    UPDATE DayWisePayHour
                    SET DayStatus=?, ARADayStatus=?, AdditionalStatus=?,
                        UpdatedBy=?, DateUpdated=GETDATE()
                    WHERE EmployeeCode=? AND WorkDate=? AND IsDefault=1
                """, (
                    'LV', 'LV', leave_short, 'Admin',
                    emp_code, cur_date
                ))

        else:
            # insert new leave row
            cur.execute("""
                INSERT INTO DayWisePayHour
                (EmployeeCode, WorkDate, ShiftID, ShiftInTime, ShiftOutTime, PayHour,
                    DayStatus, ARADayStatus, IsDefault, AdditionalStatus, HeadValue,
                    DateAdded, AddedBy)
                VALUES (?, ?, ?, ?, ?, 0,
                        ?, ?, 1, ?, 100,
                        GETDATE(), ?)
            """, (
                emp_code, cur_date,
                shift_id, shift_in, shift_out,
                'LV', 'LV', leave_short,
                'Admin'
            ))

        cur_date += timedelta(days=1)


def cancel_kormee_attendance(doc, cur, emp_code, fd, td):

    from datetime import timedelta, date

    today = date.today()
    cur_date = fd

    while cur_date <= td:

        # check row exists
        cur.execute("""
            SELECT TOP 1 DayStatus
            FROM DayWisePayHour
            WHERE EmployeeCode=? AND WorkDate=? AND IsDefault=1
        """, (emp_code, cur_date))

        row = cur.fetchone()

        # If no row exists → nothing to delete or update
        if not row:
            cur_date += timedelta(days=1)
            continue

        # ---------- CASE 1: Future date → DELETE ----------
        if cur_date > today:
            cur.execute("""
                DELETE FROM DayWisePayHour
                WHERE EmployeeCode=? AND WorkDate=? AND IsDefault=1
            """, (emp_code, cur_date))

        else:
            # ---------- CASE 2: Today or Past → UPDATE Absent ----------
            cur.execute("""
                UPDATE DayWisePayHour
                SET 
                    DayStatus='A',
                    ARADayStatus='A',
                    AdditionalStatus=NULL,
                    HeadValue=100,
                    UpdatedBy='Admin',
                    DateUpdated=GETDATE()
                WHERE EmployeeCode=? AND WorkDate=? AND IsDefault=1
            """, (emp_code, cur_date))

        cur_date += timedelta(days=1)


def delete_emp_attendance(doc):
    conn = None
    cur = None
    try:
        conn = mssql_connect(doc)
        cur = conn.cursor()

        # resolve employee code
        emp_code = get_employee_code(doc)

        fd = getdate(doc.from_date)
        td = getdate(doc.to_date)

        cancel_kormee_attendance(doc, cur, emp_code, fd, td)

        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        frappe.log_error(f"Cancel Sync Failed: {e}", "LeaveApplication.on_cancel")
    finally:
        if cur: cur.close()
        if conn: conn.close()


def delete_empleave_on_cancel(doc):
    """
    Delete EmpLeaveTransactionApproved rows for this employee + date range
    when Leave Application is cancelled.
    """
    import datetime
    conn = None
    cur = None

    try:
        # --- Employee code resolve ---
        emp_code = frappe.db.get_value("Employee", doc.employee, "employee_number") or str(doc.employee)

        # --- Normalize from/to dates ---
        fd = frappe.utils.getdate(doc.from_date)
        td = frappe.utils.getdate(doc.to_date)

        fd_str = fd.strftime("%Y-%m-%d")
        td_str = td.strftime("%Y-%m-%d")

        # --- Connect MSSQL ---
        conn = mssql_connect(doc)
        cur = conn.cursor()

        # --- Delete the leave transaction ---
        cur.execute("""
            DELETE FROM EmpLeaveTransactionApproved
            WHERE EmployeeCode = ?
            AND CAST(FromDate AS DATE) = ?
            AND CAST(ToDate AS DATE) = ?
        """, (emp_code, fd_str, td_str))

        deleted = cur.rowcount
        conn.commit()

        frappe.logger().info(
            f"[Cancel] Deleted {deleted if deleted != -1 else 'unknown'} "
            f"rows from EmpLeaveTransactionApproved for Emp={emp_code} "
            f"{fd_str} → {td_str}"
        )

    except Exception as ex:
        frappe.log_error(f"[Cancel] Delete failed ({emp_code} {fd_str}->{td_str}): {ex}",
                        "delete_empleave_on_cancel")

    finally:
        if cur: 
            try: cur.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass
