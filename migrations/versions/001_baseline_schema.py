"""Baseline schema — all 20 OASIS tables.

Revision ID: 001_baseline
Revises: None
Create Date: 2026-06-12
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001_baseline"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ORGANIZATION_MST",
        sa.Column("ORG_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("ORG_NAME", sa.Text, nullable=False),
        sa.Column("ORG_SHORT_NAME", sa.Text),
        sa.Column("ORG_ADDRESS", sa.Text),
        sa.Column("ORG_CITY", sa.Text),
        sa.Column("ORG_STATE", sa.Text),
        sa.Column("ORG_COUNTRY", sa.Text, server_default="KE"),
        sa.Column("ORG_PIN", sa.Text),
        sa.Column("ORG_PHONE", sa.Text),
        sa.Column("ORG_EMAIL", sa.Text),
        sa.Column("CURRENCY_CD", sa.Text, server_default="KES"),
        sa.Column("GST_NO", sa.Text),
        sa.Column("LEVEL_NUMBER", sa.Integer, server_default="1"),
        sa.Column("PARENT_ORG_CD", sa.Text),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
    )

    op.create_table(
        "ITEM_MST",
        sa.Column("ITM_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("ITM_LONG_NAME", sa.Text, nullable=False),
        sa.Column("ITM_SHORT_NAME", sa.Text),
        sa.Column("SCAN_ITM_CD", sa.Text),
        sa.Column("HSN_CD", sa.Text),
        sa.Column("UOM_CD", sa.Text, server_default="EA"),
        sa.Column("UOM_DESC", sa.Text, server_default="Each"),
        sa.Column("ITM_GROUP_CD", sa.Text),
        sa.Column("ITM_TYPE", sa.Text, server_default="F"),
        sa.Column("TAX_PLAN_CD", sa.Text),
        sa.Column("WEIGHT_FLAG", sa.Text, server_default="N"),
        sa.Column("SERIAL_FLAG", sa.Text, server_default="N"),
        sa.Column("PRODUCTION_FLAG", sa.Text, server_default="N"),
        sa.Column("CATEGORY", sa.Text),
        sa.Column("DEPARTMENT", sa.Text),
        sa.Column("SUPPLIER_CD", sa.Text),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
    )

    op.create_table(
        "CUSTOMER_MST",
        sa.Column("CUST_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("CUST_NAME", sa.Text),
        sa.Column("CUST_SALUTATION", sa.Text),
        sa.Column("MOBILE_NO", sa.Text),
        sa.Column("EMAIL", sa.Text),
        sa.Column("LOYALTY_CARD_NO", sa.Text),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
    )

    op.create_table(
        "TAX_PLAN_HDR",
        sa.Column("TAX_PLAN_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("TAX_PLAN_NAME", sa.Text),
        sa.Column("TAX_PLAN_TYPE", sa.Text, server_default="GST"),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
    )

    op.create_table(
        "SUPPLIER_MST",
        sa.Column("SUPPLIER_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("SUPPLIER_NAME", sa.Text, nullable=False),
        sa.Column("CONTACT_PERSON", sa.Text),
        sa.Column("PHONE", sa.Text),
        sa.Column("EMAIL", sa.Text),
        sa.Column("ADDRESS", sa.Text),
        sa.Column("PAYMENT_TERMS", sa.Text, server_default="NET30"),
        sa.Column("ORDER_FREQUENCY", sa.Text, server_default="weekly"),
        sa.Column("LEAD_TIME_DAYS", sa.Integer, server_default="7"),
        sa.Column("RELIABILITY_SCORE", sa.Float, server_default="0.9"),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
    )

    op.create_table(
        "STOCK_MASTER",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("SM_ORG_CD", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD"), primary_key=True),
        sa.Column("SM_ITM_CD", sa.Text, sa.ForeignKey("ITEM_MST.ITM_CD"), primary_key=True),
        sa.Column("SM_LOC_CD", sa.Text, primary_key=True, server_default="MAIN"),
        sa.Column("SM_QTY", sa.Float, server_default="0"),
        sa.Column("SM_WAC", sa.Float, server_default="0"),
        sa.Column("SM_LAST_RECV_DT", sa.Text),
        sa.Column("SM_LAST_ISSUE_DT", sa.Text),
    )

    op.create_table(
        "BASIC_SP_MST",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("BSP_ORG_CD", sa.Text, primary_key=True),
        sa.Column("BSP_ITEM_CD", sa.Text, sa.ForeignKey("ITEM_MST.ITM_CD"), primary_key=True),
        sa.Column("BSP_SP", sa.Float, server_default="0"),
        sa.Column("BSP_MRP", sa.Float, server_default="0"),
        sa.Column("BSP_EFF_DATE", sa.Text),
    )

    op.create_table(
        "BASIC_CP_MST",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("BCP_ORG_CD", sa.Text, primary_key=True),
        sa.Column("BCP_ITEM_CD", sa.Text, sa.ForeignKey("ITEM_MST.ITM_CD"), primary_key=True),
        sa.Column("BCP_CP", sa.Float, server_default="0"),
        sa.Column("BCP_EFF_DATE", sa.Text),
    )

    op.create_table(
        "POS_SALES_HDR",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("ORG_CD", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD"), primary_key=True),
        sa.Column("BILL_NO", sa.Text, primary_key=True),
        sa.Column("BILL_DT", sa.Text, primary_key=True),
        sa.Column("CUST_CD", sa.Text),
        sa.Column("COUNTER_CD", sa.Text),
        sa.Column("LEVEL_NUMBER", sa.Integer, server_default="1"),
        sa.Column("TOTAL_QTY", sa.Float, server_default="0"),
        sa.Column("TOTAL_AMT", sa.Float, server_default="0"),
        sa.Column("NET_AMT", sa.Float, server_default="0"),
        sa.Column("TAX_AMT", sa.Float, server_default="0"),
        sa.Column("DISC_AMT", sa.Float, server_default="0"),
        sa.Column("PAYMENT_MODE", sa.Text, server_default="CASH"),
        sa.Column("VOID_FLAG", sa.Text, server_default="F"),
        sa.Column("CUS_REF_CODE", sa.Text),
        sa.Column("CUS_REF_REMARKS", sa.Text),
    )

    op.create_table(
        "POS_SALES_DTL",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("ORG_CD", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD"), primary_key=True),
        sa.Column("BILL_NO", sa.Text, primary_key=True),
        sa.Column("BILL_DT", sa.Text, primary_key=True),
        sa.Column("SERIAL_NO", sa.Integer, primary_key=True),
        sa.Column("ITM_CD", sa.Text, sa.ForeignKey("ITEM_MST.ITM_CD")),
        sa.Column("ITEM_NAME", sa.Text),
        sa.Column("QTY", sa.Float, server_default="0"),
        sa.Column("SELL_PRICE", sa.Float, server_default="0"),
        sa.Column("NET_AMT", sa.Float, server_default="0"),
        sa.Column("TAX_AMT", sa.Float, server_default="0"),
        sa.Column("DISC_AMT", sa.Float, server_default="0"),
        sa.Column("NET_TAX_AMT", sa.Float, server_default="0"),
        sa.Column("TOTAL_VALUE", sa.Float, server_default="0"),
        sa.Column("UOM_CD", sa.Text, server_default="EA"),
        sa.Column("UOM_DESC", sa.Text, server_default="Each"),
        sa.Column("VOID_FLAG", sa.Text, server_default="F"),
        sa.Column("PROMO_ITEM_FLAG", sa.Text, server_default="N"),
        sa.Column("SCAN_ITM_CD", sa.Text),
        sa.Column("TAX_PLAN_CD", sa.Text),
    )

    op.create_table(
        "BI_SALES_REPORT",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("ORG_CD", sa.Text, primary_key=True),
        sa.Column("ITM_CD", sa.Text, primary_key=True),
        sa.Column("REPORT_MONTH", sa.Text, primary_key=True),
        sa.Column("QUANTITY", sa.Float, server_default="0"),
        sa.Column("TAX_INCL", sa.Float, server_default="0"),
        sa.Column("TAX_EXCL", sa.Float, server_default="0"),
        sa.Column("TOTAL_TAX", sa.Float, server_default="0"),
        sa.Column("COST", sa.Float, server_default="0"),
        sa.Column("TRANSACTION_COUNT", sa.Integer, server_default="0"),
    )

    op.create_table(
        "COUNTER_MST",
        sa.Column("COUNTER_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("COUNTER_NAME", sa.Text),
        sa.Column("ORG_CD", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD")),
        sa.Column("ONLINE_FLAG", sa.Text, server_default="Y"),
        sa.Column("BILL_REFUND", sa.Text, server_default="Y"),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
    )

    op.create_table(
        "BASE_SYSTEM_PREFERENCES",
        sa.Column("BSP_PREF_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("BSP_PREF_DESC", sa.Text, nullable=False, unique=True),
        sa.Column("BSP_PREF_VALUE", sa.Text),
        sa.Column("BSP_MODULE", sa.Text, server_default="POS"),
    )

    op.create_table(
        "TAX_MST",
        sa.Column("TAX_CD", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("TAX_NAME", sa.Text),
        sa.Column("TAX_PERC", sa.Float, server_default="0"),
        sa.Column("TAX_TYPE", sa.Text),
        sa.Column("TAX_PLAN_CD", sa.Text, sa.ForeignKey("TAX_PLAN_HDR.TAX_PLAN_CD")),
    )

    op.create_table(
        "GRN_HDR",
        sa.Column("TENANT_ID", sa.Text, primary_key=True, server_default="default_tenant"),
        sa.Column("GRN_NO", sa.Text, primary_key=True),
        sa.Column("ORG_CD", sa.Text, primary_key=True),
        sa.Column("SUPPLIER_CD", sa.Text),
        sa.Column("GRN_DT", sa.Text, nullable=False),
        sa.Column("TOTAL_AMT", sa.Float, server_default="0"),
    )

    op.create_table(
        "INTEGRATION_PURCHASE_ORDERS",
        sa.Column("PO_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("ORG_CD", sa.Text),
        sa.Column("ITM_CD", sa.Text),
        sa.Column("PRODUCT_NAME", sa.Text),
        sa.Column("SUPPLIER_CD", sa.Text),
        sa.Column("QUANTITY", sa.Float, server_default="0"),
        sa.Column("UNIT_COST", sa.Float, server_default="0"),
        sa.Column("TOTAL_COST", sa.Float, server_default="0"),
        sa.Column("REASONING", sa.Text),
        sa.Column("STATUS", sa.Text, server_default="PENDING"),
        sa.Column("CREATED_DT", sa.Text),
        sa.Column("APPROVED_DT", sa.Text),
        sa.Column("APPROVED_BY", sa.Text),
        sa.Column("BATCH_ID", sa.Text),
    )

    op.create_table(
        "OASIS_USERS",
        sa.Column("USER_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("USERNAME", sa.Text, unique=True, nullable=False),
        sa.Column("PASSWORD_HASH", sa.Text, nullable=False),
        sa.Column("DISPLAY_NAME", sa.Text, nullable=False),
        sa.Column("ROLE", sa.Text, nullable=False),
        sa.Column("ASSIGNED_ORG", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD")),
        sa.Column("EMAIL", sa.Text),
        sa.Column("ACTIVE_FLAG", sa.Text, server_default="Y"),
        sa.Column("CREATED_DT", sa.Text),
        sa.Column("LAST_LOGIN_DT", sa.Text),
    )

    op.create_table(
        "OASIS_SESSIONS",
        sa.Column("SESSION_TOKEN", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("USERNAME", sa.Text, nullable=False),
        sa.Column("CREATED_DT", sa.Text, nullable=False),
        sa.Column("EXPIRES_DT", sa.Text, nullable=False),
        sa.Column("IP_ADDRESS", sa.Text),
    )

    op.create_table(
        "OASIS_AUDIT_LOG",
        sa.Column("LOG_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("USERNAME", sa.Text, nullable=False),
        sa.Column("ACTION", sa.Text, nullable=False),
        sa.Column("ENTITY_TYPE", sa.Text),
        sa.Column("ENTITY_ID", sa.Text),
        sa.Column("ORG_CD", sa.Text),
        sa.Column("DETAILS", sa.Text),
        sa.Column("CREATED_DT", sa.Text, nullable=False),
    )

    op.create_table(
        "OASIS_SYSTEM_CONFIG",
        sa.Column("CONFIG_KEY", sa.Text, primary_key=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("CONFIG_VALUE", sa.Text, nullable=False),
        sa.Column("CONFIG_GROUP", sa.Text, server_default="general"),
        sa.Column("DESCRIPTION", sa.Text),
        sa.Column("UPDATED_BY", sa.Text),
        sa.Column("UPDATED_DT", sa.Text),
    )

    op.create_table(
        "INTEGRATION_TRANSFER_ORDERS",
        sa.Column("TRANSFER_ID", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("TENANT_ID", sa.Text, server_default="default_tenant"),
        sa.Column("FROM_ORG_CD", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD"), nullable=False),
        sa.Column("TO_ORG_CD", sa.Text, sa.ForeignKey("ORGANIZATION_MST.ORG_CD"), nullable=False),
        sa.Column("ITM_CD", sa.Text, nullable=False),
        sa.Column("PRODUCT_NAME", sa.Text),
        sa.Column("QUANTITY", sa.Float, server_default="0"),
        sa.Column("VALUE_KES", sa.Float, server_default="0"),
        sa.Column("STATUS", sa.Text, server_default="REQUESTED"),
        sa.Column("URGENCY", sa.Text, server_default="NORMAL"),
        sa.Column("REQUESTED_BY", sa.Text),
        sa.Column("CREATED_DT", sa.Text),
        sa.Column("COMPLETED_DT", sa.Text),
    )


def downgrade() -> None:
    op.drop_table("INTEGRATION_TRANSFER_ORDERS")
    op.drop_table("OASIS_SYSTEM_CONFIG")
    op.drop_table("OASIS_AUDIT_LOG")
    op.drop_table("OASIS_SESSIONS")
    op.drop_table("OASIS_USERS")
    op.drop_table("INTEGRATION_PURCHASE_ORDERS")
    op.drop_table("GRN_HDR")
    op.drop_table("TAX_MST")
    op.drop_table("BASE_SYSTEM_PREFERENCES")
    op.drop_table("COUNTER_MST")
    op.drop_table("BI_SALES_REPORT")
    op.drop_table("POS_SALES_DTL")
    op.drop_table("POS_SALES_HDR")
    op.drop_table("BASIC_CP_MST")
    op.drop_table("BASIC_SP_MST")
    op.drop_table("STOCK_MASTER")
    op.drop_table("SUPPLIER_MST")
    op.drop_table("TAX_PLAN_HDR")
    op.drop_table("CUSTOMER_MST")
    op.drop_table("ITEM_MST")
    op.drop_table("ORGANIZATION_MST")
