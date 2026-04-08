"""
Visual editor for `src/project_config/*.json` (project-wide financial knowledge; used by schema generation).

Run: streamlit run src/interfaces/streamlit_config_editor.py

Saves call `validate_financial_schema_configs()`; on failure the previous file content is restored.
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from src.schema.config_store import load_schema_config, schema_config_path, write_schema_config
from src.schema.financial_config_validate import validate_financial_schema_configs
from src.schema.onboarding import (
    SUPPORTED_NORMALIZED_TYPES,
    _default_core_columns,
    normalize_identifier,
)

_NORMALIZED_TYPE_OPTIONS: tuple[str, ...] = tuple(sorted(SUPPORTED_NORMALIZED_TYPES))


def _valid_concept_ids() -> set[str]:
    """All concept_id values in concepts.json (for inline validation)."""

    return {str(c["concept_id"]) for c in load_schema_config("concepts.json").get("concepts", [])}


def _level1_domain_ids() -> list[str]:
    """domain_taxonomy.json Level-1 domain_id list (for new concept primary_domain_id)."""

    tax = load_schema_config("domain_taxonomy.json")
    out: list[str] = []
    for d in tax.get("level1_domains", []):
        if isinstance(d, dict):
            did = str(d.get("domain_id", "")).strip()
            if did:
                out.append(did)
    return out


def _parse_concept_id_list(text: str) -> list[str]:
    return [p.strip() for p in text.replace("\n", ",").split(",") if p.strip()]


def _unknown_concept_refs(parts: list[str], valid: set[str]) -> list[str]:
    return [p for p in parts if p not in valid]


def _render_inline_concept_list_validation(label: str, session_key: str, valid: set[str]) -> None:
    """Show whether comma/newline-separated concept ids exist (runs every rerun = on each edit)."""

    raw = str(st.session_state.get(session_key, ""))
    parts = _parse_concept_id_list(raw)
    if not parts:
        st.caption(f"_{label}: empty (optional or fill before save)._")
        return
    unknown = _unknown_concept_refs(parts, valid)
    if unknown:
        st.warning(f"{label}: unknown concept id(s) — {unknown}")
    else:
        st.caption(f"✓ {label}: all {len(parts)} id(s) exist in concepts.json.")


def _backup_write_validate(config_name: str, payload: dict[str, object]) -> tuple[bool, str]:
    path = schema_config_path(config_name=config_name)
    backup = path.read_text(encoding="utf-8")
    write_schema_config(config_name=config_name, payload=payload)
    try:
        validate_financial_schema_configs()
        return True, ""
    except ValueError as exc:
        path.write_text(backup, encoding="utf-8")
        return False, str(exc)


def _backup_write_validate_many(
    writes: list[tuple[str, dict[str, object]]],
) -> tuple[bool, str]:
    """Write multiple config files then validate; restore all on failure."""

    backups: dict[str, str] = {}
    for config_name, _ in writes:
        path = schema_config_path(config_name=config_name)
        backups[config_name] = path.read_text(encoding="utf-8")
    try:
        for config_name, payload in writes:
            write_schema_config(config_name=config_name, payload=payload)
        validate_financial_schema_configs()
        return True, ""
    except ValueError as exc:
        for config_name, content in backups.items():
            schema_config_path(config_name=config_name).write_text(content, encoding="utf-8")
        return False, str(exc)


def _ensure_minimal_field_packs(
    field_packs_payload: dict[str, object],
    concept_id: str,
) -> tuple[str, str, list[str]]:
    """
    Create pk_{concept_id} and {concept_id}_core in packs if missing.
    Returns (pk_pack_name, core_pack_name, created_pack_names).
    """

    packs_obj = field_packs_payload.get("packs")
    if not isinstance(packs_obj, dict):
        packs_obj = {}
        field_packs_payload["packs"] = packs_obj
    packs: dict[str, object] = packs_obj
    pk_name = f"pk_{concept_id}"
    core_name = f"{concept_id}_core"
    created: list[str] = []
    if pk_name not in packs:
        packs[pk_name] = [
            {"name": f"{concept_id}_id", "normalized_type": "string", "is_primary_key": True},
        ]
        created.append(pk_name)
    if core_name not in packs:
        packs[core_name] = _default_core_columns(concept_id=concept_id)
        created.append(core_name)
    return pk_name, core_name, created


def _packs_from_payload(payload: dict[str, object]) -> dict[str, list[dict[str, object]]]:
    """Load packs for editor state; each column gets a stable _editor_id (stripped on save)."""

    packs_raw = payload.get("packs", {})
    if not isinstance(packs_raw, dict):
        return {}
    result: dict[str, list[dict[str, object]]] = {}
    for pack_name, columns in packs_raw.items():
        pk = str(pack_name)
        if not isinstance(columns, list):
            result[pk] = []
            continue
        cols: list[dict[str, object]] = []
        for col in columns:
            if not isinstance(col, dict):
                continue
            name = str(col.get("name", "")).strip()
            nt = str(col.get("normalized_type", "string")).lower().strip()
            if nt not in SUPPORTED_NORMALIZED_TYPES:
                nt = "string"
            entry: dict[str, object] = {
                "name": name,
                "normalized_type": nt,
                "_editor_id": str(uuid.uuid4()),
            }
            if bool(col.get("is_primary_key", False)):
                entry["is_primary_key"] = True
            av = col.get("allowed_values")
            if isinstance(av, list) and av:
                entry["allowed_values"] = [str(x).strip() for x in av if str(x).strip()]
        result[pk] = cols
    return result


def _strip_editor_ids_and_serialize(
    packs: dict[str, list[dict[str, object]]],
) -> dict[str, object]:
    out_packs: dict[str, object] = {}
    for pack_name, columns in packs.items():
        serialized: list[dict[str, object]] = []
        for col in columns:
            clean = {k: v for k, v in col.items() if k != "_editor_id"}
            if "is_primary_key" in clean and not clean["is_primary_key"]:
                del clean["is_primary_key"]
            serialized.append(clean)
        out_packs[pack_name] = serialized
    return {"packs": out_packs}


def _concepts_referencing_pack(pack_name: str) -> list[str]:
    concepts_payload = load_schema_config("concepts.json")
    refs: list[str] = []
    for concept in concepts_payload.get("concepts", []):
        if not isinstance(concept, dict):
            continue
        cid = str(concept.get("concept_id", ""))
        required = [str(x) for x in concept.get("required_packs", [])]
        optional = [str(x) for x in concept.get("optional_packs", [])]
        if pack_name in required or pack_name in optional:
            refs.append(cid)
    return sorted(refs)


def _flush_pack_widgets_into_state(pack_name: str) -> None:
    """Copy widget values for one pack into fp_packs_state (call when switching pack or before save)."""

    packs: dict[str, list[dict[str, object]]] = st.session_state.fp_packs_state
    cols = packs.get(pack_name, [])
    new_cols: list[dict[str, object]] = []
    for col in cols:
        rid = str(col.get("_editor_id", ""))
        if not rid:
            continue
        name_key = f"fp_name_{rid}"
        type_key = f"fp_type_{rid}"
        pk_key = f"fp_pk_{rid}"
        name_val = str(st.session_state.get(name_key, col.get("name", ""))).strip()
        type_val = str(st.session_state.get(type_key, col.get("normalized_type", "string"))).lower()
        if type_val not in SUPPORTED_NORMALIZED_TYPES:
            type_val = "string"
        pk_val = bool(st.session_state.get(pk_key, col.get("is_primary_key", False)))
        rebuilt: dict[str, object] = {
            "name": name_val,
            "normalized_type": type_val,
            "_editor_id": rid,
        }
        if pk_val:
            rebuilt["is_primary_key"] = True
        av_raw = str(st.session_state.get(f"fp_av_{rid}", "")).strip()
        if type_val == "categorical" and av_raw:
            parts = [p.strip() for p in av_raw.replace("\n", ",").split(",") if p.strip()]
            if parts:
                rebuilt["allowed_values"] = parts
        new_cols.append(rebuilt)
    packs[pack_name] = new_cols


def _fp_on_pack_selector_change() -> None:
    previous = st.session_state.get("_fp_pack_prev")
    current = st.session_state.fp_pack_selector
    if previous is not None and previous != current and previous in st.session_state.fp_packs_state:
        _flush_pack_widgets_into_state(previous)
    st.session_state._fp_pack_prev = current


def _validate_packs_for_save(packs: dict[str, list[dict[str, object]]]) -> tuple[bool, str]:
    for pack_name, columns in packs.items():
        seen_names: set[str] = set()
        for col in columns:
            clean = {k: v for k, v in col.items() if k != "_editor_id"}
            name = str(clean.get("name", "")).strip()
            if not name:
                return False, f"Pack {pack_name!r}: empty column name."
            if name in seen_names:
                return False, f"Pack {pack_name!r}: duplicate column name {name!r}."
            seen_names.add(name)
            nt = str(clean.get("normalized_type", "")).lower()
            if nt not in SUPPORTED_NORMALIZED_TYPES:
                return False, f"Pack {pack_name!r}: invalid normalized_type {nt!r} for column {name!r}."
            av = clean.get("allowed_values")
            if av is not None:
                if nt != "categorical":
                    return False, f"Pack {pack_name!r} column {name!r}: allowed_values only for categorical."
                if not isinstance(av, list) or not av:
                    return False, f"Pack {pack_name!r} column {name!r}: allowed_values must be a non-empty list when set."
                if len(av) != len(set(str(x) for x in av)):
                    return False, f"Pack {pack_name!r} column {name!r}: duplicate allowed_values."
        if not columns:
            return False, f"Pack {pack_name!r}: at least one column is required."
    return True, ""


def _field_packs_management_tab() -> None:
    st.subheader("Field packs")
    st.caption(
        "Edit **field_packs.json** packs (column lists). Switching pack auto-applies edits to the previous pack. "
        "Column names are checked for empty/duplicate on each edit. For **categorical**, optional "
        "**allowed_values** (comma-separated) becomes a DDL `CHECK (... IN (...))` and guides synthesis. "
        "Save runs full-repo validation. You cannot delete a pack still referenced by **concepts.json**."
    )

    if "fp_packs_state" not in st.session_state:
        st.session_state.fp_packs_state = _packs_from_payload(load_schema_config("field_packs.json"))

    packs: dict[str, list[dict[str, object]]] = st.session_state.fp_packs_state
    pack_names_sorted = sorted(packs.keys())

    c_reload, c_spacer = st.columns([1, 3])
    with c_reload:
        if st.button("Reload field_packs.json from disk", key="fp_reload_disk"):
            st.session_state.fp_packs_state = _packs_from_payload(load_schema_config("field_packs.json"))
            if "_fp_pack_prev" in st.session_state:
                del st.session_state._fp_pack_prev
            st.rerun()

    if not pack_names_sorted:
        st.warning("No packs yet. Create one below.")
    else:
        if "fp_pack_selector" not in st.session_state:
            st.session_state.fp_pack_selector = pack_names_sorted[0]
        if "_fp_pack_prev" not in st.session_state:
            st.session_state._fp_pack_prev = st.session_state.fp_pack_selector

        st.selectbox(
            "Pack to edit",
            options=pack_names_sorted,
            key="fp_pack_selector",
            on_change=_fp_on_pack_selector_change,
        )
        selected = str(st.session_state.fp_pack_selector)

        refs = _concepts_referencing_pack(selected)
        if refs:
            st.caption(f"Referenced by concepts: {', '.join(refs)}")

        columns = packs.get(selected, [])
        st.markdown(f"**Columns** in `{selected}` ({len(columns)})")

        for col in columns:
            rid = str(col.get("_editor_id", ""))
            if not rid:
                continue
            default_name = str(col.get("name", ""))
            default_type = str(col.get("normalized_type", "string"))
            if default_type not in SUPPORTED_NORMALIZED_TYPES:
                default_type = "string"
            default_pk = bool(col.get("is_primary_key", False))
            with st.expander(f"Column: {default_name or '(unnamed)'}", expanded=False):
                st.text_input("name", value=default_name, key=f"fp_name_{rid}")
                st.selectbox(
                    "normalized_type",
                    options=_NORMALIZED_TYPE_OPTIONS,
                    index=_NORMALIZED_TYPE_OPTIONS.index(default_type)
                    if default_type in _NORMALIZED_TYPE_OPTIONS
                    else 0,
                    key=f"fp_type_{rid}",
                )
                st.checkbox("is_primary_key", value=default_pk, key=f"fp_pk_{rid}")
                av_list = col.get("allowed_values")
                av_default = ", ".join(str(x) for x in av_list) if isinstance(av_list, list) else ""
                st.text_input(
                    "allowed_values (comma-separated; use for categorical → DDL CHECK + sampling)",
                    value=av_default,
                    key=f"fp_av_{rid}",
                    help="Leave empty for unconstrained categorical at DDL level.",
                )
                if st.button("Remove this column", key=f"fp_rmcol_{rid}"):
                    _flush_pack_widgets_into_state(selected)
                    packs[selected] = [c for c in packs[selected] if str(c.get("_editor_id")) != rid]
                    st.rerun()

        names_in_pack: list[str] = []
        for col in columns:
            rid2 = str(col.get("_editor_id", ""))
            if not rid2:
                continue
            nm = str(st.session_state.get(f"fp_name_{rid2}", col.get("name", ""))).strip()
            names_in_pack.append(nm)
        if names_in_pack:
            if any(n == "" for n in names_in_pack):
                st.warning("Some columns have an empty **name** — fix before Save.")
            non_empty = [n for n in names_in_pack if n]
            if len(non_empty) != len(set(non_empty)):
                st.warning("Duplicate **column names** in this pack — fix before Save.")
            elif not any(n == "" for n in names_in_pack):
                st.caption("✓ Column names in this pack are non-empty and unique (types are constrained by selectbox).")

        if st.button("Add column to this pack", key="fp_add_col"):
            _flush_pack_widgets_into_state(selected)
            packs[selected].append(
                {
                    "name": "new_column",
                    "normalized_type": "string",
                    "_editor_id": str(uuid.uuid4()),
                }
            )
            st.rerun()

        if st.button("Delete entire pack", key="fp_del_pack"):
            if refs:
                st.error(f"Cannot delete: still referenced by concepts: {', '.join(refs)}")
            else:
                _flush_pack_widgets_into_state(selected)
                del packs[selected]
                names_after = sorted(packs.keys())
                if names_after:
                    st.session_state.fp_pack_selector = names_after[0]
                st.session_state._fp_pack_prev = st.session_state.fp_pack_selector
                st.rerun()

    st.markdown("**New pack**")
    new_pack_raw = st.text_input(
        "New pack name (normalized like concept_id)",
        placeholder="e.g. my_custom_core",
        key="fp_new_pack_name",
    )
    np_preview = normalize_identifier(new_pack_raw) if new_pack_raw.strip() else ""
    if new_pack_raw.strip():
        if not np_preview:
            st.warning("Pack name cannot be normalized to a valid identifier.")
        elif np_preview in packs:
            st.warning(f"Pack `{np_preview}` already exists.")
        else:
            st.caption(f"✓ Will create pack `{np_preview}` on **Create pack**.")

    if st.button("Create pack with one placeholder column", key="fp_create_pack"):
        new_id = normalize_identifier(new_pack_raw) if new_pack_raw.strip() else ""
        if not new_id:
            st.error("Invalid or empty pack name.")
            return
        if new_id in packs:
            st.error(f"Pack {new_id!r} already exists.")
            return
        packs[new_id] = [
            {
                "name": "column_name",
                "normalized_type": "string",
                "_editor_id": str(uuid.uuid4()),
            }
        ]
        st.session_state.fp_pack_selector = new_id
        st.session_state._fp_pack_prev = new_id
        st.rerun()

    if st.button("Save field_packs.json", type="primary", key="fp_save_all"):
        _flush_pack_widgets_into_state(str(st.session_state.fp_pack_selector))
        ok_struct, err_msg = _validate_packs_for_save(st.session_state.fp_packs_state)
        if not ok_struct:
            st.error(err_msg)
            return
        payload = _strip_editor_ids_and_serialize(st.session_state.fp_packs_state)
        ok, err = _backup_write_validate("field_packs.json", payload)
        if ok:
            st.success("Saved and full config validates.")
            st.session_state.fp_packs_state = _packs_from_payload(load_schema_config("field_packs.json"))
            st.session_state._fp_pack_prev = st.session_state.fp_pack_selector
        else:
            st.error(err)


def _json_text_editor_tab(config_name: str, title: str, help_text: str) -> None:
    st.subheader(title)
    st.caption(help_text)
    payload = load_schema_config(config_name=config_name)
    text_key = f"json_text_{config_name}"
    if text_key not in st.session_state:
        st.session_state[text_key] = json.dumps(payload, indent=2, ensure_ascii=True)
    edited = st.text_area(
        label=f"{config_name} (JSON)",
        value=st.session_state[text_key],
        height=420,
        key=f"ta_{config_name}",
    )
    st.session_state[text_key] = edited
    try:
        parsed: dict[str, object] = json.loads(edited)
        st.caption("✓ JSON syntax is valid (Save still runs full-repo validation).")
        with st.expander("Parsed preview", expanded=False):
            st.json(parsed)
    except json.JSONDecodeError as exc:
        st.warning(f"JSON not valid yet: {exc}")
        parsed = {}
    c1, c2 = st.columns(2)
    with c1:
        if st.button(f"Save {config_name}", key=f"save_{config_name}"):
            try:
                parsed = json.loads(edited)
            except json.JSONDecodeError as exc:
                st.error(f"Invalid JSON: {exc}")
                return
            ok, err = _backup_write_validate(config_name=config_name, payload=parsed)
            if ok:
                st.success("Saved and full config validates.")
            else:
                st.error(err)
    with c2:
        if st.button(f"Reload from disk", key=f"reload_{config_name}"):
            st.session_state[text_key] = json.dumps(load_schema_config(config_name), indent=2, ensure_ascii=True)
            st.rerun()


def _domain_extension_rules_form_tab() -> None:
    st.subheader("Domain extension rules")
    st.caption(
        "When scenario/system concepts satisfy **when_matched_contains_all**, append **append_concepts**. "
        "Concept ids must exist in concepts.json. **Each field is checked on every edit** (before Save). "
        "Save still runs full-repo validation."
    )
    payload = load_schema_config("domain_extension_rules.json")
    valid_concepts = _valid_concept_ids()
    description = str(payload.get("description", ""))
    st.markdown(description)

    if "der_rules" not in st.session_state:
        st.session_state.der_rules = json.loads(json.dumps(payload.get("rules", [])))

    rules: list[dict[str, object]] = st.session_state.der_rules
    for index, rule in enumerate(rules):
        with st.expander(f"{rule.get('id', 'rule')} (#{index + 1})", expanded=False):
            rule["id"] = st.text_input("rule id", value=str(rule.get("id", "")), key=f"der_id_{index}")
            when_list = list(rule.get("when_matched_contains_all", []))
            append_list = list(rule.get("append_concepts", []))
            st.text_input(
                "when_matched_contains_all (comma-separated concept ids)",
                value=", ".join(str(x) for x in when_list),
                key=f"der_when_{index}",
            )
            _render_inline_concept_list_validation(
                "when_matched_contains_all",
                f"der_when_{index}",
                valid_concepts,
            )
            st.text_input(
                "append_concepts (comma-separated)",
                value=", ".join(str(x) for x in append_list),
                key=f"der_app_{index}",
            )
            _render_inline_concept_list_validation(
                "append_concepts",
                f"der_app_{index}",
                valid_concepts,
            )
            rule["business_rationale"] = st.text_area(
                "business_rationale",
                value=str(rule.get("business_rationale", "")),
                height=100,
                key=f"der_rat_{index}",
            )
            if st.button("Remove this rule", key=f"der_rm_{index}"):
                rules.pop(index)
                st.rerun()

    if st.button("Add empty rule"):
        rules.append(
            {
                "id": "new_rule",
                "when_matched_contains_all": [],
                "append_concepts": [],
                "business_rationale": "",
            }
        )
        st.rerun()

    if st.button("Save domain_extension_rules.json", type="primary"):
        rebuilt: list[dict[str, object]] = []
        rule_count = len(st.session_state.der_rules)
        for index in range(rule_count):
            when_raw = st.session_state.get(f"der_when_{index}", "")
            app_raw = st.session_state.get(f"der_app_{index}", "")
            when_parts = [p.strip() for p in when_raw.split(",") if p.strip()]
            app_parts = [p.strip() for p in app_raw.split(",") if p.strip()]
            rebuilt.append(
                {
                    "id": str(st.session_state.get(f"der_id_{index}", "rule")),
                    "when_matched_contains_all": when_parts,
                    "append_concepts": app_parts,
                    "business_rationale": str(st.session_state.get(f"der_rat_{index}", "")),
                }
            )
        out_payload: dict[str, object] = {
            "description": description,
            "rules": rebuilt,
        }
        ok, err = _backup_write_validate("domain_extension_rules.json", out_payload)
        if ok:
            st.success("Saved and full config validates.")
            st.session_state.der_rules = json.loads(json.dumps(rebuilt))
        else:
            st.error(err)


def _system_profiles_form_tab() -> None:
    st.subheader("System profiles")
    st.caption("Each profile lists **default_concepts** and **required_concepts** (subset of defaults).")
    payload = load_schema_config("system_profiles.json")
    all_concept_ids = [
        str(c["concept_id"]) for c in load_schema_config("concepts.json").get("concepts", [])
    ]
    profiles = dict(payload.get("profiles", {}))
    updated: dict[str, object] = {}
    for profile_name, profile_body in profiles.items():
        st.markdown(f"**{profile_name}**")
        defaults = list(profile_body.get("default_concepts", []))
        required = list(profile_body.get("required_concepts", []))
        new_defaults = st.multiselect(
            f"default_concepts ({profile_name})",
            options=all_concept_ids,
            default=[x for x in defaults if x in all_concept_ids],
            key=f"sp_def_{profile_name}",
        )
        new_required = st.multiselect(
            f"required_concepts ({profile_name})",
            options=new_defaults,
            default=[x for x in required if x in new_defaults],
            key=f"sp_req_{profile_name}",
        )
        updated[profile_name] = {
            "default_concepts": new_defaults,
            "required_concepts": new_required,
        }
    if st.button("Save system_profiles.json", type="primary"):
        ok, err = _backup_write_validate("system_profiles.json", {"profiles": updated})
        if ok:
            st.success("Saved and full config validates.")
        else:
            st.error(err)


def _concept_aliases_tab() -> None:
    st.subheader("Concept aliases")
    st.caption("Edit **aliases** for scenario text matching only. Packs and table names stay in JSON on disk.")
    payload = load_schema_config("concepts.json")
    concepts = list(payload.get("concepts", []))
    labels = [f"{c['concept_id']} → {c.get('default_table_name', '')}" for c in concepts]
    choice = st.selectbox("Concept", range(len(concepts)), format_func=lambda i: labels[i])
    concept = concepts[int(choice)]
    aliases = list(concept.get("aliases", []))
    text = st.text_area(
        "Aliases (one per line or comma-separated — will be split and de-duplicated)",
        value="\n".join(aliases),
        height=200,
        key="concept_aliases_area",
    )
    if st.button("Save aliases to concepts.json", type="primary"):
        lines: list[str] = []
        for part in text.replace(",", "\n").split("\n"):
            stripped = part.strip().lower()
            if stripped:
                lines.append(stripped)
        seen: set[str] = set()
        unique = []
        for item in lines:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        concept["aliases"] = unique
        ok, err = _backup_write_validate("concepts.json", payload)
        if ok:
            st.success("Saved and full config validates.")
        else:
            st.error(err)


def _add_concept_tab() -> None:
    st.subheader("Add a new concept")
    st.caption(
        "Appends one entry to **concepts.json**. Optionally create **pk_{concept_id}** and **{concept_id}_core** in "
        "**field_packs.json** (same pattern as onboarding). You can add more packs via multiselect. "
        "You may still need **concept_relation_graph.json** / **system_profiles.json** for FKs and profiles. "
        "**Live checks** below run on every edit (Save still runs full validation)."
    )
    payload = load_schema_config("concepts.json")
    concepts = list(payload.get("concepts", []))
    existing_ids = {str(c["concept_id"]) for c in concepts}
    existing_tables = {str(c.get("default_table_name", "")).lower() for c in concepts}

    field_packs = load_schema_config("field_packs.json")
    pack_names = sorted(field_packs.get("packs", {}).keys())

    st.text_input(
        "concept_id",
        placeholder="e.g. collateral_pledge or Collateral Pledge",
        help="Normalized to snake_case (same rules as onboarding).",
        key="ac_raw_cid",
    )
    raw_concept_id = str(st.session_state.get("ac_raw_cid", ""))
    concept_id = normalize_identifier(raw_concept_id) if raw_concept_id.strip() else ""
    if raw_concept_id.strip() and concept_id:
        st.caption(f"Normalized: `{concept_id}`")
    st.text_input(
        "default_table_name",
        placeholder="e.g. collateral_pledges",
        help="Physical table name in generated DDL.",
        key="ac_table",
    )
    default_table = str(st.session_state.get("ac_table", ""))
    st.text_input(
        "aliases (comma or newline separated, optional)",
        placeholder="collateral, pledge, collateral pledge",
        key="ac_aliases",
    )
    aliases_text = str(st.session_state.get("ac_aliases", ""))

    domain_options = _level1_domain_ids()
    st.selectbox(
        "primary_domain_id (required; see domain_taxonomy.json)",
        options=domain_options if domain_options else ["party_legal_relationship"],
        index=0,
        key="ac_primary_domain",
    )

    st.markdown("**Live checks** (before Save)")
    if raw_concept_id.strip():
        if not concept_id:
            st.warning("concept_id cannot be normalized to a valid identifier.")
        elif concept_id in existing_ids:
            st.warning(f"concept_id `{concept_id}` already exists in concepts.json.")
        else:
            st.caption(f"✓ concept_id `{concept_id}` is not in use yet.")
    if default_table.strip():
        if default_table.strip().lower() in existing_tables:
            st.warning(f"default_table_name `{default_table.strip()}` is already used.")
        else:
            st.caption(f"✓ Table name `{default_table.strip()}` is not used by another concept.")

    generate_minimal_packs = st.checkbox(
        "Create minimal packs in field_packs if missing: "
        "`pk_{concept_id}` (single surrogate key) + `{concept_id}_core` (status/amount/time/json)",
        value=True,
    )
    if generate_minimal_packs and concept_id:
        st.caption(
            f"Pack names: `pk_{concept_id}`, `{concept_id}_core` — aligned with `onboarding._default_core_columns`."
        )

    additional_required = st.multiselect(
        "Additional required_packs (optional; FK packs like fk_customer must be added here if needed)",
        options=pack_names,
        help="Ignored as duplicate if same as auto pk/core when minimal packs are enabled.",
    )
    optional_packs = st.multiselect("optional_packs", options=pack_names)

    manual_required = st.multiselect(
        "required_packs (only when minimal packs is off)",
        options=pack_names,
        help="When unchecked above, pick at least one pack manually.",
        disabled=generate_minimal_packs,
    )

    if st.button("Append concept and save", type="primary"):
        if not concept_id:
            st.error("concept_id is empty or invalid after normalization.")
            return
        if concept_id in existing_ids:
            st.error(f"concept_id `{concept_id}` already exists.")
            return
        table_key = default_table.strip()
        if not table_key:
            st.error("default_table_name is required.")
            return
        if table_key.lower() in existing_tables:
            st.error(f"default_table_name `{table_key}` is already used.")
            return

        field_packs_payload = json.loads(json.dumps(field_packs))
        if generate_minimal_packs:
            pk_name, core_name, created = _ensure_minimal_field_packs(
                field_packs_payload=field_packs_payload,
                concept_id=concept_id,
            )
            final_required: list[str] = [pk_name, core_name]
            for pack_name in additional_required:
                if pack_name not in final_required:
                    final_required.append(pack_name)
            if created:
                st.info("Will create new packs: " + ", ".join(created))
        else:
            final_required = list(manual_required)
            if not final_required:
                st.error("Either enable minimal packs or select at least one required_packs entry.")
                return

        unknown = [p for p in final_required + list(optional_packs) if p not in field_packs_payload.get("packs", {})]
        if unknown:
            st.error(f"Unknown pack names: {unknown}")
            return

        alias_lines: list[str] = []
        for part in aliases_text.replace(",", "\n").split("\n"):
            stripped = part.strip().lower()
            if stripped:
                alias_lines.append(stripped)
        if concept_id.replace("_", " ") not in alias_lines:
            alias_lines.insert(0, concept_id.replace("_", " "))
        if concept_id not in alias_lines:
            alias_lines.append(concept_id)
        seen: set[str] = set()
        unique_aliases: list[str] = []
        for item in alias_lines:
            if item not in seen:
                seen.add(item)
                unique_aliases.append(item)

        primary_domain = str(st.session_state.get("ac_primary_domain", "")).strip()
        if not primary_domain:
            st.error("primary_domain_id is required.")
            return

        new_concept: dict[str, object] = {
            "concept_id": concept_id,
            "primary_domain_id": primary_domain,
            "aliases": unique_aliases,
            "default_table_name": table_key,
            "required_packs": final_required,
            "optional_packs": list(optional_packs),
        }
        concepts.append(new_concept)
        payload["concepts"] = concepts

        writes: list[tuple[str, dict[str, object]]] = []
        if generate_minimal_packs:
            writes.append(("field_packs.json", field_packs_payload))
        writes.append(("concepts.json", payload))

        if len(writes) == 1:
            ok, err = _backup_write_validate("concepts.json", payload)
        else:
            ok, err = _backup_write_validate_many(writes)

        if ok:
            st.success(
                "Saved and full config validates. Remember concept_relation_graph / system_profiles / "
                "domain_extension_rules if this concept participates in FKs or profiles."
            )
            st.json(new_concept)
        else:
            st.error(err)


def _concept_relation_graph_readonly_tab() -> None:
    st.subheader("Concept relation graph")
    st.caption(
        "FK rules and parent/child concept closure are driven by **concept_relation_graph.json** "
        "(nodes + edges). To edit, use **More JSON files** or hand-edit the file, then run validation."
    )
    kg = load_schema_config("concept_relation_graph.json")
    st.markdown(str(kg.get("description", "")))
    nodes = list(kg.get("nodes", []))
    edges = list(kg.get("edges", []))
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Nodes", len(nodes))
    with c2:
        st.metric("Edges", len(edges))
    with st.expander("Nodes (concepts in the graph)", expanded=False):
        st.json(nodes)
    with st.expander("Edges (parent → child, FK on child)", expanded=False):
        st.json(edges)
    try:
        from src.schema.knowledge_graph import load_compiled_relation_patterns

        compiled = load_compiled_relation_patterns()
        st.caption(f"Compiled **{len(compiled)}** relation row(s) for FK inference (same shape as legacy relation list).")
        with st.expander("Compiled relation rows (read-only)", expanded=False):
            st.json(compiled)
    except (OSError, ValueError) as exc:
        st.warning(f"Could not compile graph: {exc}")


def main() -> None:
    st.set_page_config(page_title="Schema config editor", layout="wide")
    st.title("Financial schema config editor")
    st.caption(
        f"Config directory: `{schema_config_path(config_name='concepts.json').parent}`. "
        "Structured tabs validate inputs on each edit; saves still run full `validate_financial_schema_configs()`. "
        "After editing JSON elsewhere or merging, run `make validate-config`."
    )

    tab_rules, tab_prof, tab_alias, tab_add, tab_fp, tab_kg, tab_fb, tab_raw = st.tabs(
        [
            "Domain extension rules",
            "System profiles",
            "Concept aliases",
            "Add concept",
            "Field packs",
            "Concept relation graph",
            "Feedback weights (JSON)",
            "More JSON files",
        ]
    )

    with tab_rules:
        _domain_extension_rules_form_tab()

    with tab_prof:
        _system_profiles_form_tab()

    with tab_alias:
        _concept_aliases_tab()

    with tab_add:
        _add_concept_tab()

    with tab_fp:
        _field_packs_management_tab()

    with tab_kg:
        _concept_relation_graph_readonly_tab()

    with tab_fb:
        _json_text_editor_tab(
            "feedback_weights.json",
            "Feedback weights",
            "Optional **alias_weights** and **concept_bias** used in concept scoring.",
        )

    with tab_raw:
        st.caption("Edit other JSON configs as raw text. Prefer small, careful changes.")
        raw_files = [
            "concept_relation_graph.json",
            "business_lines.json",
            "concepts.json",
            "field_packs.json",
        ]
        raw_choice = st.selectbox("File", raw_files, key="raw_file_pick")
        text_key = f"json_text_{raw_choice}"
        if st.session_state.get("_last_raw_file") != raw_choice:
            st.session_state[text_key] = json.dumps(
                load_schema_config(raw_choice), indent=2, ensure_ascii=True
            )
            st.session_state["_last_raw_file"] = raw_choice
        st.caption("Validate on save restores file if validation fails.")
        edited = st.text_area(
            label=f"{raw_choice}",
            value=st.session_state.get(text_key, ""),
            height=420,
            key=f"ta_raw_{raw_choice}",
        )
        st.session_state[text_key] = edited
        try:
            raw_preview = json.loads(edited)
            st.caption("✓ JSON syntax is valid (Save still runs full-repo validation).")
            st.json(raw_preview)
        except json.JSONDecodeError as exc:
            st.warning(f"JSON preview unavailable: {exc}")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Save", key="save_raw", type="primary"):
                try:
                    parsed = json.loads(edited)
                except json.JSONDecodeError as exc:
                    st.error(f"Invalid JSON: {exc}")
                else:
                    ok, err = _backup_write_validate(raw_choice, parsed)
                    if ok:
                        st.success("Saved and full config validates.")
                    else:
                        st.error(err)
        with c2:
            if st.button("Reload from disk", key="reload_raw"):
                st.session_state[text_key] = json.dumps(
                    load_schema_config(raw_choice), indent=2, ensure_ascii=True
                )
                st.rerun()


if __name__ == "__main__":
    main()
