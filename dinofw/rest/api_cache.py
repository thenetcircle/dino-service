from typing import List, Optional

from dinofw.rest.models import Group


def _langs_key(spoken_languages: Optional[List[str]]) -> str:
    if not spoken_languages:
        return "none"
    langs = [s.lower() for s in spoken_languages
             if isinstance(s, str) and len(s) == 2 and s.isascii()]
    if not langs:
        return "none"
    return ",".join(sorted(set(langs)))


def _to_payload(groups: List[Group]) -> List[dict]:
    # Pydantic v2 prefers model_dump; v1 prefers dict()
    out = []
    for g in groups:
        if hasattr(g, "model_dump"):
            out.append(g.model_dump(exclude_none=True))
        else:
            out.append(g.dict(exclude_none=True))  # type: ignore[attr-defined]
    return out


def _from_payload(items: List[dict]) -> List[Group]:
    out = []
    for d in items:
        if hasattr(Group, "model_validate"):
            out.append(Group.model_validate(d))
        else:
            out.append(Group(**d))
    return out
