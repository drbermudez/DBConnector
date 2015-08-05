using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using DBInterface;

namespace DBConnectorTester
{
    public partial class frmMain : Form
    {
        private bool integratedSecurity = false;

        public frmMain()
        {
            InitializeComponent();
        }

        private void btnConnect_Click(object sender, EventArgs e)
        {
            DBConnectorMSSQL conn = new DBConnectorMSSQL(@txtServer.Text, @txtDBName.Text, @txtUsername.Text, txtPassword.Text, 
                                               Convert.ToBoolean(cmbPersist.SelectedItem), integratedSecurity, 40);

            if(conn.CanConnect())
            {
                lblMessage.Text = "Connection successful!";
            }
            else
            {
                foreach(Error error in conn.ErrorList)
                {
                    lblMessage.Text += error.Message + " " + error.RoutineName + Environment.NewLine;
                }
            }
        }

        private void btnExecute_Click(object sender, EventArgs e)
        {
            if (txtQuery.Text.Trim().Length > 0)
            {
                DBConnectorMSSQL conn = new DBConnectorMSSQL(@txtServer.Text, @txtDBName.Text, @txtUsername.Text, 
                                                   txtPassword.Text, Convert.ToBoolean(cmbPersist.SelectedItem), 
                                                   integratedSecurity, 40);

                int rows = conn.ExecuteNonQuery(txtQuery.Text, CommandType.Text);
                if(rows <= 0)
                {
                    conn.Clear();
                    //DataSet values = new DataSet();
                    DataTable values = new DataTable();
                    values = conn.GetTable(txtQuery.Text, CommandType.Text);

                    lblMessage.Text = values.Rows.Count.ToString() + " tables were affected.";
                }
                else
                {
                    lblMessage.Text = rows.ToString() + " rows were affected.";
                }

                foreach (Error error in conn.ErrorList)
                {
                    lblMessage.Text += error.Message + " " + error.RoutineName + Environment.NewLine;
                }
            }
        }

        private void chkIntegrated_CheckedChanged(object sender, EventArgs e)
        {
            txtPassword.Enabled = !chkIntegrated.Checked;
            txtUsername.Enabled = !chkIntegrated.Checked;
            integratedSecurity = chkIntegrated.Checked;
            if (chkIntegrated.Checked)
            {
                txtPassword.Clear();
                txtUsername.Clear();
            }
        }
    }
}
