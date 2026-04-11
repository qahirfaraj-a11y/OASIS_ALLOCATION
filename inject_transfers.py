import sys

with open('ops_dashboard.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_transfer_tracking = '''
    # ── SECTION C: Transfer Status Tracking ──────────────
    st.markdown("#### 🚚 Transfer Execution & Status")
    adapter = get_adapter()
    org_filter = selected_org if not user_perms.get("can_view_all_stores") else None
    df_transfers = adapter.fetch_transfers(org_filter)
    
    if not df_transfers.empty:
        # Action handler for marking received
        action_col, tbl_col = st.columns([1, 4])
        with action_col:
            st.caption("Update Status")
            transfer_id = st.number_input("Transfer ID", min_value=1, step=1)
            new_status = st.selectbox("Status", ["IN_TRANSIT", "RECEIVED"])
            if st.button("Update Status", use_container_width=True):
                if adapter.update_transfer_status(transfer_id, new_status):
                    log_action(DB_PATH, current_user['username'], "TRANSFER_EXECUTED", ENTITY_TRANSFER,
                               f"TX_{transfer_id}", selected_org, {"status": new_status})
                    st.success(f"Transfer {transfer_id} marked as {new_status}")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to update (ID not found or DB error).")
        
        with tbl_col:
            # Color code status
            def color_status(val):
                if val == 'REQUESTED': return 'color: #ff9800;'
                elif val == 'IN_TRANSIT': return 'color: #2196f3;'
                elif val == 'RECEIVED': return 'color: #4caf50;'
                return ''
                
            disp_df = df_transfers.copy()
            # formatting
            disp_df["VALUE_KES"] = disp_df["VALUE_KES"].apply(lambda x: f"{x:,.0f}")
            if "COMPLETED_DT" in disp_df.columns:
                disp_df["COMPLETED_DT"] = disp_df["COMPLETED_DT"].fillna("-")
                
            st.dataframe(disp_df.style.applymap(color_status, subset=["STATUS"]), 
                         use_container_width=True, hide_index=True, height=300)
    else:
        st.info("No transfer records found.")

'''

new_exec = '''            if st.button("🚀 Execute Live Sim Transfers", key="exec_sim_xfer"):
                adapter = get_adapter()
                items_to_push = [{"item_code": t.itm_cd, "product_name": t.product_name, "transfer_qty": t.transfer_qty, "transfer_value": t.value_kes, "urgency": t.urgency} for t in sim_transfers]
                if adapter.push_transfer_request(sim_transfers[0].from_org, selected_org, items_to_push):
                    log_action(DB_PATH, current_user["username"], "TRANSFER_EXECUTED", ENTITY_TRANSFER, f"TX_BATCH_{int(time.time())}", selected_org, {"items": len(items_to_push)})
                    st.success(f"Dispatched {len(sim_transfers)} inter-branch transfers!")
                    time.sleep(1)
                    st.rerun()
'''

out_lines = []
skip_next = False
for i, line in enumerate(lines):
    if skip_next:
        skip_next = False
        continue
        
    if 'if st.button("🚀 Execute Live Sim Transfers"' in line:
        out_lines.append(new_exec)
        skip_next = True # Skip the st.success line underneath it
    elif '# TAB 3: END-OF-DAY STOCK REVIEW' in line:
        # We need to insert the transfer tracking right before the # ======= comment block
        # Since '# TAB 3' is on line 884, but line 883 is # =======================================
        # This condition is fine, we just put it before the Tab 3 title 
        out_lines.insert(len(out_lines)-1, new_transfer_tracking)
        out_lines.append(line)
    else:
        out_lines.append(line)

with open('ops_dashboard.py', 'w', encoding='utf-8') as f:
    f.writelines(out_lines)

print("Injected Transfer tracking block")
