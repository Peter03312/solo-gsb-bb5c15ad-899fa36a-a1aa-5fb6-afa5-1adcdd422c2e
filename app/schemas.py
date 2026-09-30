"""HTTP 请求体的 Pydantic 模型与按事件类型的必填字段校验。"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

EventType = Literal["transfer", "receive", "seal_check", "storage"]
CorrectionAction = Literal["void", "replace"]

# 各事件类型的必填字段（事件上报与纠错替换共用）
REQUIRED_FIELDS: dict[str, list[str]] = {
    "transfer": ["transfer_no", "from_custodian", "to_custodian"],
    "receive": ["transfer_no", "receiver", "seal_no"],
    "seal_check": ["checked_seal_no"],
    "storage": [],
}


class SampleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sample_code: str = Field(..., min_length=1, description="样本编号，业务唯一键")
    collected_at: datetime = Field(..., description="采集时间 ISO8601")
    collector: str = Field(..., min_length=1, description="采集人，即初始保管人")
    seal_no: str = Field(..., min_length=1, description="封签号")


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(..., min_length=1, description="全局唯一事件号，同号重试幂等")
    event_type: EventType
    sample_code: str = Field(..., min_length=1, description="样本编号")
    occurred_at: datetime = Field(..., description="事件发生时间 ISO8601")

    # transfer（移交）
    transfer_no: str | None = Field(None, description="移交单号（transfer 必填）")
    from_custodian: str | None = Field(None, description="移交发起/交出方（transfer 必填，须为当前保管人）")
    to_custodian: str | None = Field(None, description="接收方（transfer 必填）")

    # receive（签收）
    receiver: str | None = Field(None, description="实际签收人（receive 必填，须等于移交的接收方）")
    seal_no: str | None = Field(None, description="签收核对封签号（receive 必填，须与登记一致）")

    # seal_check（封签检查）
    checked_seal_no: str | None = Field(None, description="检查时读取的封签号（seal_check 必填）")
    seal_intact: bool | None = Field(None, description="封签是否完好，默认 true；false 判无效")

    # storage（实验室入库）
    storage_location: str | None = Field(None, description="入库库位（storage 建议填写）")
    custodian: str | None = Field(None, description="入库后保管人，默认实验室")

    note: str | None = Field(None, description="备注，参与幂等指纹")

    @model_validator(mode="after")
    def _check_required_fields(self) -> "EventIn":
        missing = [f for f in REQUIRED_FIELDS[self.event_type] if not getattr(self, f)]
        if missing:
            raise ValueError(f"事件类型 {self.event_type} 缺少必填字段: {', '.join(missing)}")
        return self


class NewEventIn(BaseModel):
    """纠错替换时提交的完整新事件内容（不含 sample_code —— 纠错不得跨样本）。

    字段集与校验规则与 EventIn 一致；新事件号必须全局唯一且未被占用。
    """

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(..., min_length=1, description="新事件号，必须未被占用（全局唯一）")
    event_type: EventType
    occurred_at: datetime = Field(..., description="事件发生时间 ISO8601")

    # transfer（移交）
    transfer_no: str | None = Field(None, description="移交单号（transfer 必填）")
    from_custodian: str | None = Field(None, description="移交发起/交出方（transfer 必填，须为当前保管人）")
    to_custodian: str | None = Field(None, description="接收方（transfer 必填）")

    # receive（签收）
    receiver: str | None = Field(None, description="实际签收人（receive 必填，须等于移交的接收方）")
    seal_no: str | None = Field(None, description="签收核对封签号（receive 必填，须与登记一致）")

    # seal_check（封签检查）
    checked_seal_no: str | None = Field(None, description="检查时读取的封签号（seal_check 必填）")
    seal_intact: bool | None = Field(None, description="封签是否完好，默认 true；false 判无效")

    # storage（实验室入库）
    storage_location: str | None = Field(None, description="入库库位（storage 建议填写）")
    custodian: str | None = Field(None, description="入库后保管人，默认实验室")

    note: str | None = Field(None, description="备注，参与幂等指纹")

    @model_validator(mode="after")
    def _check_required_fields(self) -> "NewEventIn":
        missing = [f for f in REQUIRED_FIELDS[self.event_type] if not getattr(self, f)]
        if missing:
            raise ValueError(f"事件类型 {self.event_type} 缺少必填字段: {', '.join(missing)}")
        return self


class CorrectionIn(BaseModel):
    """核定前事件纠错：作废（void）或替换（replace）。

    * 仅核定入库前可纠错，纠错不得跨样本；
    * 同一纠错号同内容重试幂等（200），异内容冲突（409）；
    * replace 必须给出完整新事件内容与未占用的新事件号；void 不得携带新事件。
    """

    model_config = ConfigDict(extra="forbid")

    correction_id: str = Field(..., min_length=1, description="全局唯一纠错号，同号重试幂等")
    sample_code: str = Field(..., min_length=1, description="样本编号（纠错不得跨样本）")
    target_event_id: str = Field(..., min_length=1, description="被作废/替换的当前有效事件号")
    action: CorrectionAction = Field(..., description="void=作废，replace=替换")
    operator: str = Field(..., min_length=1, description="纠错操作人")
    reason: str = Field(..., min_length=1, description="纠错原因")
    new_event: NewEventIn | None = Field(None, description="替换时的完整新事件内容；作废时必须为空")

    @model_validator(mode="after")
    def _check_action_payload(self) -> "CorrectionIn":
        if self.action == "replace" and self.new_event is None:
            raise ValueError("action=replace 必须提供 new_event（完整新事件内容与未占用的新事件号）")
        if self.action == "void" and self.new_event is not None:
            raise ValueError("action=void 作废不得携带 new_event")
        return self


class ApproveIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approver: str = Field(..., min_length=1, description="核定操作人")
    note: str | None = Field(None, description="核定备注")


class TransferBatchItemIn(BaseModel):
    """车次批量移交中的一份样本：编号 + 该份移交的事件号与移交单号。"""

    model_config = ConfigDict(extra="forbid")

    sample_code: str = Field(..., min_length=1, description="本份样本编号")
    event_id: str = Field(..., min_length=1, description="本份移交的事件号，全局唯一（既有 transfer 事件）")
    transfer_no: str = Field(..., min_length=1, description="本份移交单（移交凭证）号")


class TransferBatchIn(BaseModel):
    """同一交接车次多份样本的批量移交：整批一次提交、原子成败。

    * 批内样本编号不得重复、事件号不得重复（结构校验失败 422，不落任何事件/批次）；
      移交单号按样本各自核验，批内不要求唯一；
    * 各份移交统一使用批次级 handed_over_at / from_custodian / to_custodian；
    * 每份样本按既有 transfer 事件落库（事件内容与 5.2 单事件接口完全一致）；
    * 同 batch_id 同内容重试幂等（200，即使样本已核定）；同 batch_id 异内容 409。
    """

    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(..., min_length=1, description="全局唯一批次号（同一交接车次），同号重试幂等")
    handed_over_at: datetime = Field(..., description="统一移交时间 ISO8601")
    from_custodian: str = Field(..., min_length=1, description="交出人（须为每份样本当前保管人，否则核验判无效）")
    to_custodian: str = Field(..., min_length=1, description="接收人")
    items: list[TransferBatchItemIn] = Field(..., min_length=1, description="本车次涉及的每份样本")

    @model_validator(mode="after")
    def _check_batch_duplicates(self) -> "TransferBatchIn":
        sample_codes = [it.sample_code for it in self.items]
        dup_samples = sorted({c for c in sample_codes if sample_codes.count(c) > 1})
        if dup_samples:
            raise ValueError(f"批内样本编号重复: {', '.join(dup_samples)}")
        event_ids = [it.event_id for it in self.items]
        dup_events = sorted({e for e in event_ids if event_ids.count(e) > 1})
        if dup_events:
            raise ValueError(f"批内事件号重复: {', '.join(dup_events)}")
        return self
