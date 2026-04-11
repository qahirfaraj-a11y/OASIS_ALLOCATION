import sys

with open('ops_dashboard_indented.py', 'r', encoding='utf-8') as f:
    indented_lines = f.readlines()

new_top = '''    can_approve = user_perms.get("can_approve_po", False)
    if can_approve:
        t_gen, t_app = st.tabs(["🛠️ Generate Orders", "✅ Pending Approvals"])
    else:
        from contextlib import nullcontext
        t_gen = nullcontext()
        t_app = None

    with t_gen:
'''

new_push_button = '''            if pos_recs:
                colA, colB = st.columns(2)
                with colA:
                    if st.button("🚀 Push to PENDING Approvals", type="primary", use_container_width=True):
                        with st.spinner("Pushing..."):
                            adapter = get_adapter()
                            pushed = adapter.push_purchase_order(selected_org, pos_recs)
                            if pushed:
                                log_action(DB_PATH, current_user['username'], ACTION_PO_GENERATED,
                                           ENTITY_PO, f"PO_{selected_org}_{int(time.time())}", selected_org,
                                           {"items": pushed})
                                st.success(f"Sent {pushed} items to pending approvals.")
                                time.sleep(1)
                                st.rerun()
                with colB:
                    df_csv = pd.DataFrame(pos_recs)
                    csv_data = df_csv.to_csv(index=False)
                    if st.download_button("📥 Export CSV Backup", data=csv_data, file_name="po.csv", use_container_width=True):
                        pass

'''

new_bottom = '''
    if t_app is not None:
        with t_app:
            st.markdown("#### 📋 Purchase Orders Awaiting Approval")
            adapter = get_adapter()
            org_filter = selected_org if not user_perms.get("can_view_all_stores") else None
            df_pending = adapter.fetch_pending_pos(org_filter)
            
            if df_pending.empty:
                st.info("No pending purchase orders waiting for approval.")
            else:
                st.caption("You can edit the **QUANTITY** column. A reason is required if you modify the quantity.")
                
                edit_df = df_pending.copy()
                edit_df.insert(0, "Select", False)
                edit_df.insert(len(edit_df.columns), "Reason", "")
                
                edited_df = st.data_editor(
                    edit_df,
                    hide_index=True,
                    use_container_width=True,
                    disabled=[c for c in edit_df.columns if c not in ["Select", "QUANTITY", "Reason"]],
                    column_config={"Select": st.column_config.CheckboxColumn("Select", required=True)}
                )
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Approve Selected", type="primary", use_container_width=True):
                        selected = edited_df[edited_df["Select"] == True]
                        if not selected.empty:
                            count = 0
                            for _, row in selected.iterrows():
                                po_id = row["PO_ID"]
                                orig_qty = df_pending[df_pending["PO_ID"] == po_id].iloc[0]["QUANTITY"]
                                new_qty = row["QUANTITY"]
                                reason = row["Reason"] if new_qty != orig_qty else None
                                
                                if adapter.update_po_status(po_id, "APPROVED", current_user["username"], new_qty, reason):
                                    count += 1
                                    log_action(DB_PATH, current_user["username"], "PO_APPROVED", ENTITY_PO,
                                               f"PO_ID_{po_id}", row["ORG_CD"], {"new_qty": new_qty, "reason": reason})
                            st.success(f"Approved {count} purchase orders.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("No rows selected.")
                with col2:
                    if st.button("❌ Reject Selected", use_container_width=True):
                        selected = edited_df[edited_df["Select"] == True]
                        if not selected.empty:
                            count = 0
                            for _, row in selected.iterrows():
                                po_id = row["PO_ID"]
                                if adapter.update_po_status(po_id, "REJECTED", current_user["username"]):
                                    count += 1
                                    log_action(DB_PATH, current_user["username"], "PO_REJECTED", ENTITY_PO,
                                               f"PO_ID_{po_id}", row["ORG_CD"], {})
                            st.success(f"Rejected {count} purchase orders.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("No rows selected.")
'''

with open('ops_dashboard.py', 'r', encoding='utf-8') as f:
    orig_lines = f.readlines()

out_lines = orig_lines[:926]
out_lines.append(new_top)

# The indented lines from 926 to 1085
for i in range(926, 1086):
    out_lines.append(indented_lines[i])

# Our new push block
out_lines.append(new_push_button)

# The new bottom approvals tab
out_lines.append(new_bottom)

# The rest of the file
out_lines.extend(orig_lines[1094:])

with open('ops_dashboard.py', 'w', encoding='utf-8') as f:
    f.writelines(out_lines)

print("ops_dashboard.py successfully rebuilt")
