"""
SQLAlchemy ORM models for the OASIS database schema.

These models serve as the single source of truth for the database schema.
Alembic auto-generates migrations by diffing these models against the DB.
The mock_pos_erp.py builder uses raw SQL for SQLite seeding, but this
module is the canonical reference for PostgreSQL and migration tooling.
"""

from sqlalchemy import Column, Integer, Float, Text, ForeignKey, MetaData
from sqlalchemy.orm import declarative_base

metadata = MetaData()
Base = declarative_base(metadata=metadata)


class OrganizationMst(Base):
    __tablename__ = "ORGANIZATION_MST"
    ORG_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    ORG_NAME = Column(Text, nullable=False)
    ORG_SHORT_NAME = Column(Text)
    ORG_ADDRESS = Column(Text)
    ORG_CITY = Column(Text)
    ORG_STATE = Column(Text)
    ORG_COUNTRY = Column(Text, default="KE")
    ORG_PIN = Column(Text)
    ORG_PHONE = Column(Text)
    ORG_EMAIL = Column(Text)
    CURRENCY_CD = Column(Text, default="KES")
    GST_NO = Column(Text)
    LEVEL_NUMBER = Column(Integer, default=1)
    PARENT_ORG_CD = Column(Text)
    ACTIVE_FLAG = Column(Text, default="Y")


class ItemMst(Base):
    __tablename__ = "ITEM_MST"
    ITM_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    ITM_LONG_NAME = Column(Text, nullable=False)
    ITM_SHORT_NAME = Column(Text)
    SCAN_ITM_CD = Column(Text)
    HSN_CD = Column(Text)
    UOM_CD = Column(Text, default="EA")
    UOM_DESC = Column(Text, default="Each")
    ITM_GROUP_CD = Column(Text)
    ITM_TYPE = Column(Text, default="F")
    TAX_PLAN_CD = Column(Text)
    WEIGHT_FLAG = Column(Text, default="N")
    SERIAL_FLAG = Column(Text, default="N")
    PRODUCTION_FLAG = Column(Text, default="N")
    CATEGORY = Column(Text)
    DEPARTMENT = Column(Text)
    SUPPLIER_CD = Column(Text)
    ACTIVE_FLAG = Column(Text, default="Y")


class StockMaster(Base):
    __tablename__ = "STOCK_MASTER"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    SM_ORG_CD = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"), primary_key=True)
    SM_ITM_CD = Column(Text, ForeignKey("ITEM_MST.ITM_CD"), primary_key=True)
    SM_LOC_CD = Column(Text, primary_key=True, default="MAIN")
    SM_QTY = Column(Float, default=0)
    SM_WAC = Column(Float, default=0)
    SM_LAST_RECV_DT = Column(Text)
    SM_LAST_ISSUE_DT = Column(Text)


class BasicSpMst(Base):
    __tablename__ = "BASIC_SP_MST"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    BSP_ORG_CD = Column(Text, primary_key=True)
    BSP_ITEM_CD = Column(Text, ForeignKey("ITEM_MST.ITM_CD"), primary_key=True)
    BSP_SP = Column(Float, default=0)
    BSP_MRP = Column(Float, default=0)
    BSP_EFF_DATE = Column(Text)


class BasicCpMst(Base):
    __tablename__ = "BASIC_CP_MST"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    BCP_ORG_CD = Column(Text, primary_key=True)
    BCP_ITEM_CD = Column(Text, ForeignKey("ITEM_MST.ITM_CD"), primary_key=True)
    BCP_CP = Column(Float, default=0)
    BCP_EFF_DATE = Column(Text)


class CustomerMst(Base):
    __tablename__ = "CUSTOMER_MST"
    CUST_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    CUST_NAME = Column(Text)
    CUST_SALUTATION = Column(Text)
    MOBILE_NO = Column(Text)
    EMAIL = Column(Text)
    LOYALTY_CARD_NO = Column(Text)
    ACTIVE_FLAG = Column(Text, default="Y")


class PosSalesHdr(Base):
    __tablename__ = "POS_SALES_HDR"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    ORG_CD = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"), primary_key=True)
    BILL_NO = Column(Text, primary_key=True)
    BILL_DT = Column(Text, primary_key=True)
    CUST_CD = Column(Text)
    COUNTER_CD = Column(Text)
    LEVEL_NUMBER = Column(Integer, default=1)
    TOTAL_QTY = Column(Float, default=0)
    TOTAL_AMT = Column(Float, default=0)
    NET_AMT = Column(Float, default=0)
    TAX_AMT = Column(Float, default=0)
    DISC_AMT = Column(Float, default=0)
    PAYMENT_MODE = Column(Text, default="CASH")
    VOID_FLAG = Column(Text, default="F")
    CUS_REF_CODE = Column(Text)
    CUS_REF_REMARKS = Column(Text)


class PosSalesDtl(Base):
    __tablename__ = "POS_SALES_DTL"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    ORG_CD = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"), primary_key=True)
    BILL_NO = Column(Text, primary_key=True)
    BILL_DT = Column(Text, primary_key=True)
    SERIAL_NO = Column(Integer, primary_key=True)
    ITM_CD = Column(Text, ForeignKey("ITEM_MST.ITM_CD"))
    ITEM_NAME = Column(Text)
    QTY = Column(Float, default=0)
    SELL_PRICE = Column(Float, default=0)
    NET_AMT = Column(Float, default=0)
    TAX_AMT = Column(Float, default=0)
    DISC_AMT = Column(Float, default=0)
    NET_TAX_AMT = Column(Float, default=0)
    TOTAL_VALUE = Column(Float, default=0)
    UOM_CD = Column(Text, default="EA")
    UOM_DESC = Column(Text, default="Each")
    VOID_FLAG = Column(Text, default="F")
    PROMO_ITEM_FLAG = Column(Text, default="N")
    SCAN_ITM_CD = Column(Text)
    TAX_PLAN_CD = Column(Text)


class BiSalesReport(Base):
    __tablename__ = "BI_SALES_REPORT"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    ORG_CD = Column(Text, primary_key=True)
    ITM_CD = Column(Text, primary_key=True)
    REPORT_MONTH = Column(Text, primary_key=True)
    QUANTITY = Column(Float, default=0)
    TAX_INCL = Column(Float, default=0)
    TAX_EXCL = Column(Float, default=0)
    TOTAL_TAX = Column(Float, default=0)
    COST = Column(Float, default=0)
    TRANSACTION_COUNT = Column(Integer, default=0)


class CounterMst(Base):
    __tablename__ = "COUNTER_MST"
    COUNTER_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    COUNTER_NAME = Column(Text)
    ORG_CD = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"))
    ONLINE_FLAG = Column(Text, default="Y")
    BILL_REFUND = Column(Text, default="Y")
    ACTIVE_FLAG = Column(Text, default="Y")


class BaseSystemPreferences(Base):
    __tablename__ = "BASE_SYSTEM_PREFERENCES"
    BSP_PREF_ID = Column(Integer, primary_key=True, autoincrement=True)
    TENANT_ID = Column(Text, default="default_tenant")
    BSP_PREF_DESC = Column(Text, nullable=False, unique=True)
    BSP_PREF_VALUE = Column(Text)
    BSP_MODULE = Column(Text, default="POS")


class TaxPlanHdr(Base):
    __tablename__ = "TAX_PLAN_HDR"
    TAX_PLAN_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    TAX_PLAN_NAME = Column(Text)
    TAX_PLAN_TYPE = Column(Text, default="GST")
    ACTIVE_FLAG = Column(Text, default="Y")


class TaxMst(Base):
    __tablename__ = "TAX_MST"
    TAX_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    TAX_NAME = Column(Text)
    TAX_PERC = Column(Float, default=0)
    TAX_TYPE = Column(Text)
    TAX_PLAN_CD = Column(Text, ForeignKey("TAX_PLAN_HDR.TAX_PLAN_CD"))


class SupplierMst(Base):
    __tablename__ = "SUPPLIER_MST"
    SUPPLIER_CD = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    SUPPLIER_NAME = Column(Text, nullable=False)
    CONTACT_PERSON = Column(Text)
    PHONE = Column(Text)
    EMAIL = Column(Text)
    ADDRESS = Column(Text)
    PAYMENT_TERMS = Column(Text, default="NET30")
    ORDER_FREQUENCY = Column(Text, default="weekly")
    LEAD_TIME_DAYS = Column(Integer, default=7)
    RELIABILITY_SCORE = Column(Float, default=0.9)
    ACTIVE_FLAG = Column(Text, default="Y")


class GrnHdr(Base):
    __tablename__ = "GRN_HDR"
    TENANT_ID = Column(Text, primary_key=True, default="default_tenant")
    GRN_NO = Column(Text, primary_key=True)
    ORG_CD = Column(Text, primary_key=True)
    SUPPLIER_CD = Column(Text)
    GRN_DT = Column(Text, nullable=False)
    TOTAL_AMT = Column(Float, default=0)


class IntegrationPurchaseOrders(Base):
    __tablename__ = "INTEGRATION_PURCHASE_ORDERS"
    PO_ID = Column(Integer, primary_key=True, autoincrement=True)
    TENANT_ID = Column(Text, default="default_tenant")
    ORG_CD = Column(Text)
    ITM_CD = Column(Text)
    PRODUCT_NAME = Column(Text)
    SUPPLIER_CD = Column(Text)
    QUANTITY = Column(Float, default=0)
    UNIT_COST = Column(Float, default=0)
    TOTAL_COST = Column(Float, default=0)
    REASONING = Column(Text)
    STATUS = Column(Text, default="PENDING")
    CREATED_DT = Column(Text)
    APPROVED_DT = Column(Text)
    APPROVED_BY = Column(Text)
    BATCH_ID = Column(Text)


class OasisUsers(Base):
    __tablename__ = "OASIS_USERS"
    USER_ID = Column(Integer, primary_key=True, autoincrement=True)
    TENANT_ID = Column(Text, default="default_tenant")
    USERNAME = Column(Text, unique=True, nullable=False)
    PASSWORD_HASH = Column(Text, nullable=False)
    DISPLAY_NAME = Column(Text, nullable=False)
    ROLE = Column(Text, nullable=False)
    ASSIGNED_ORG = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"))
    EMAIL = Column(Text)
    ACTIVE_FLAG = Column(Text, default="Y")
    CREATED_DT = Column(Text)
    LAST_LOGIN_DT = Column(Text)


class OasisSessions(Base):
    __tablename__ = "OASIS_SESSIONS"
    SESSION_ID = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    USERNAME = Column(Text, nullable=False)
    CREATED_DT = Column(Text, nullable=False)
    EXPIRES_DT = Column(Text, nullable=False)
    IS_REVOKED = Column(Integer, server_default="0", default=0)


class OasisAuditLog(Base):
    __tablename__ = "OASIS_AUDIT_LOG"
    LOG_ID = Column(Integer, primary_key=True, autoincrement=True)
    TENANT_ID = Column(Text, default="default_tenant")
    USERNAME = Column(Text, nullable=False)
    ACTION = Column(Text, nullable=False)
    ENTITY_TYPE = Column(Text)
    ENTITY_ID = Column(Text)
    ORG_CD = Column(Text)
    DETAILS = Column(Text)
    CREATED_DT = Column(Text, nullable=False)


class OasisSystemConfig(Base):
    __tablename__ = "OASIS_SYSTEM_CONFIG"
    CONFIG_KEY = Column(Text, primary_key=True)
    TENANT_ID = Column(Text, default="default_tenant")
    CONFIG_VALUE = Column(Text, nullable=False)
    CONFIG_GROUP = Column(Text, default="general")
    DESCRIPTION = Column(Text)
    UPDATED_BY = Column(Text)
    UPDATED_DT = Column(Text)


class IntegrationTransferOrders(Base):
    __tablename__ = "INTEGRATION_TRANSFER_ORDERS"
    TRANSFER_ID = Column(Integer, primary_key=True, autoincrement=True)
    TENANT_ID = Column(Text, default="default_tenant")
    FROM_ORG_CD = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"), nullable=False)
    TO_ORG_CD = Column(Text, ForeignKey("ORGANIZATION_MST.ORG_CD"), nullable=False)
    ITM_CD = Column(Text, nullable=False)
    PRODUCT_NAME = Column(Text)
    QUANTITY = Column(Float, default=0)
    VALUE_KES = Column(Float, default=0)
    STATUS = Column(Text, default="REQUESTED")
    URGENCY = Column(Text, default="NORMAL")
    REQUESTED_BY = Column(Text)
    CREATED_DT = Column(Text)
    COMPLETED_DT = Column(Text)
