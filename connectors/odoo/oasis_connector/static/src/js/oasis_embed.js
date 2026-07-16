/** @odoo-module **/
// OASIS console embed — a minimal client action that renders a console
// (Streamlit, ?embed=true) in an iframe filling Odoo's content area.

import { registry } from "@web/core/registry";
import { Component } from "@odoo/owl";

export class OasisEmbed extends Component {
    setup() {
        this.url = (this.props.action && this.props.action.params
                    && this.props.action.params.url) || "";
    }
}
OasisEmbed.template = "oasis_connector.Embed";

registry.category("actions").add("oasis_embed", OasisEmbed);
