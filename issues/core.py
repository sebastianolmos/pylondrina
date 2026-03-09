from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Type, Mapping

from ..reports import Issue
from ..errors import PylondrinaError  

IssueContext = dict[str, Any]
BuildDetailsFn = Callable[[IssueContext], dict[str, Any]]

@dataclass(frozen=True)
class IssueSpec:
    code: str
    level: str  # "info"|"warning"|"error"
    message_template: str
    details_keys: tuple[str, ...] = ()
    defaults: dict[str, Any] = field(default_factory=dict)
    build_details: Optional[BuildDetailsFn] = None
    fatal: bool = False
    exception: Optional[str] = None  # "schema"|"import"|...

    def render_message(self, ctx: IssueContext) -> str:
        # template con {field!r}, {rule!r}, etc.
        return self.message_template.format_map(ctx)

    def make_details(self, ctx: IssueContext) -> Optional[dict[str, Any]]:
        if self.build_details:
            d = self.build_details(ctx)
            return d if d else None
        if not self.details_keys:
            return None
        d = {k: ctx.get(k) for k in self.details_keys if k in ctx}
        return d if d else None


def emit_issue(
    issues: list[Issue],
    catalog: dict[str, "IssueSpec"],
    code: str,
    *,
    message_override: Optional[str] = None,
    field: Optional[str] = None,
    source_field: Optional[str] = None,
    row_count: Optional[int] = None,
    details: Optional[dict[str, Any]] = None,
    **ctx: Any,
) -> Issue:
    if code not in catalog:
        # en desarrollo prefiero ValueError para pillar typos rápido
        raise ValueError(f"Unknown issue code: {code}")

    spec = catalog[code]

    # construir contexto
    full_ctx = dict(spec.defaults)
    full_ctx.update(ctx)
    if field is not None:
        full_ctx["field"] = field
    if source_field is not None:
        full_ctx["source_field"] = source_field
    if row_count is not None:
        full_ctx["row_count"] = row_count

    # mensaje
    message = message_override or spec.render_message(full_ctx)

    # details: manual gana, si no hay manual, usar spec.make_details(ctx)
    final_details = details if details is not None else spec.make_details(full_ctx)

    issue = Issue(
        level=spec.level,
        code=spec.code,
        message=message,
        field=field,
        source_field=source_field,
        row_count=row_count,
        details=final_details,
    )
    issues.append(issue)
    return issue

def emit_and_maybe_raise(
    issues: list[Issue],
    catalog: dict[str, IssueSpec],
    code: str,
    *,
    strict: bool,
    exception_map: Mapping[str, Type[PylondrinaError]],
    default_exception: Type[PylondrinaError],
    message_override: Optional[str] = None,
    field: Optional[str] = None,
    source_field: Optional[str] = None,
    row_count: Optional[int] = None,
    details: Optional[dict[str, Any]] = None,
    **ctx: Any,
) -> Issue:
    """
    Emite Issue y, si corresponde por política (fatal/strict), levanta excepción.

    Política:
    - spec.fatal == True -> siempre levanta
    - si no fatal: strict==True y issue.level=="error" -> levanta
    """
    issue = emit_issue(
        issues, catalog, code,
        message_override=message_override,
        field=field,
        source_field=source_field,
        row_count=row_count,
        details=details,
        **ctx,
    )

    spec = catalog[code]
    should_raise = spec.fatal or (strict and issue.level == "error")
    if not should_raise:
        return issue

    exc_cls = default_exception
    if spec.exception is not None and spec.exception in exception_map:
        exc_cls = exception_map[spec.exception]

    raise exc_cls(
        issue.message,
        code=issue.code,
        details=issue.details,
        issue=issue,
        issues=issues,
    )