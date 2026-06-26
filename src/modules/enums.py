from enum import Enum

class Status(Enum):
    """隔離データの対応ステータス"""
    OPEN = "OPEN"                 # 未対応（初期値）
    IN_PROGRESS = "IN_PROGRESS" # 調査・修正中
    REMEDIATED = "REMEDIATED"   # 修正・再投入完了
    IGNORED = "IGNORED"         # 対応不要（破棄）


class ErrorSeverity(Enum):
    """エラーの深刻度（ダッシュボードの色分けやアラート用）"""
    CRITICAL = "CRITICAL"       # システム稼働に影響あり（主キー欠損、スキーマ崩壊）
    ERROR = "ERROR"             # 業務影響あり（日付パースエラー、値の異常値）
    WARNING = "WARNING"         # 軽微（将来の拡張用など）


class QuarantineReasonCode(Enum):
    """隔離理由のマスターコード（テスト仕様書・コード共通化用）"""
    SCHEMA_VIOLATION    = "SCHEMA_VIOLATION"    # Auto Loaderによるスキーマ破壊検知（_rescued_dataあり）
    MISSING_PRIMARY_KEY = "MISSING_PRIMARY_KEY"   # 必須属性（主キー）の欠損（log_idがNULL）
    TYPE_CAST_FAILURE   = "TYPE_CAST_FAILURE"     # 日付フォーマット等の型変換エラー（parsed_tsがNULL）
    INVALID_VALUE_RANGE = "INVALID_VALUE_RANGE"   # ビジネスルール違反・異常値（cycle_time_secの不正値）