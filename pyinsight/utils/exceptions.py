"""
Insight Module Error Code Description:
INS-000001: No Topic / No Data
INS-000002: No Data Type description
INS-000003: No Data Specification Found
INS-000004: Data Store not supported
INS-000005: Messager Type Error
INS-000006: Depositor Type Error
INS-000007: Archiver Type Error
INS-000008: Translator Type Error
"""
class InsightTypeError(Exception): pass
class InsightEncodeError(Exception): pass
class InsightClientConfigError(Exception): pass
class InsightDataSpecError(Exception): pass