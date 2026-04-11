import sys

with open('ops_dashboard.py', 'r', encoding='utf-8') as f:
    orig_lines = f.readlines()

new_header_code = '''
# ── Notification System ──
from oasis.logic.notification_service import NotificationService

if 'notification_service' not in st.session_state:
    st.session_state['notification_service'] = NotificationService(get_connector(), None)
    
notif_service = st.session_state['notification_service']

# Determine context for alerts
alert_org_cd = None if user_perms.get("can_view_all_stores") else user_org
active_alerts = notif_service.get_active_alerts(org_cd=alert_org_cd, user_role=user_role, username=current_user['username'])

unread_alerts = [a for a in active_alerts if not a.get("is_read")]

if unread_alerts:
    # Use a toast for the newest unread alert as a push notification paradigm
    st.toast(f"🔔 {len(unread_alerts)} new system alert(s)! {unread_alerts[0]['title']}", icon="🔔")

col_title, col_alerts = st.columns([5, 1])

# Replace the old static header HTML with dynamic layout
st.markdown(f"""
<div class="header-bar" style="margin-bottom: 5px;">
    <div>
        <h1>🔮 OASIS Retail Manager</h1>
        <div class="subtitle">Operations, Allocation, Sales Intelligence & Simulation</div>
    </div>
    <div style="text-align: right; color: #ccc;">
        <div style="font-size: 1.1em; font-weight: 600;">{current_user['display_name']}</div>
        <div style="font-size: 0.85em; color: #888;">{role_labels.get(user_role, user_role)}</div>
    </div>
</div>
""", unsafe_allow_html=True)

with col_title:
    pass # Empty to push alerts to the right

with col_alerts:
    if active_alerts:
        btn_label = f"🔔 Alerts ({len(unread_alerts)})" if unread_alerts else "🔕 Inbox (0)"
        type_str = "primary" if unread_alerts else "secondary"
        with st.popover(btn_label):
            st.markdown("### System Notifications")
            for alert in active_alerts:
                # Basic styling for alerts
                color = "#f44336" if alert['urgency'] == "HIGH" else "#ff9800"
                st.markdown(f"<div style='border-left: 3px solid {color}; padding-left: 10px; margin-bottom: 10px;'>"
                            f"<strong>{alert['title']}</strong><br/>"
                            f"<span style='font-size:0.85em;color:#aaa;'>{alert['message']}</span><br/>"
                            f"<span style='font-size:0.7em;color:#666;'>{alert['timestamp']}</span>"
                            f"</div>", unsafe_allow_html=True)
                if not alert['is_read']:
                    notif_service.mark_as_read(current_user['username'], alert['id'])
            if st.button("Dismiss All", use_container_width=True):
                st.rerun()

st.markdown("<br/>", unsafe_allow_html=True)
'''

# Find the header block and replace it
# The original code looks like:
# st.markdown(f"""
# <div class="header-bar">
#     <div>
#         <h1>🔮 OASIS Retail Manager</h1>
#         ...
#     </div>
# </div>
# """, unsafe_allow_html=True)

out_lines = []
skip = False
for i, line in enumerate(orig_lines):
    if 'role_labels = {' in line:
        out_lines.append(line) # keep role_labels
    elif '    \'ops_admin\': \'🔧 Operations Admin\',' in line or '    \'regional_manager\': \'🌐 Regional Manager\',' in line or '    \'branch_manager\': \'🏪 Branch Manager\'' in line or '}' in line:
        if i > 330 and i < 340: # It's within the header definition
            out_lines.append(line)
        else:
            if not skip: out_lines.append(line)
    elif 'st.markdown(f"""' in line and i > 330 and i < 340: # start of header
        skip = True
        out_lines.append(new_header_code)
    elif '""", unsafe_allow_html=True)' in line and skip:
        skip = False # end of header, resume output
    else:
        if not skip:
            out_lines.append(line)

with open('ops_dashboard.py', 'w', encoding='utf-8') as f:
    f.writelines(out_lines)

print("Injected Notification UI into dashboard header.")
