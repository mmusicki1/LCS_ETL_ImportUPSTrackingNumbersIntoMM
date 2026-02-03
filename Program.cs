using System;
using System.Collections.Generic;
using CommonModules;
using ProcessEConnectPacket;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;

namespace LCS_ETL_ImportUPSTrackingNumbersIntoMM
{
	class Program
	{
		static string DBA_Local_DBName = "DBA_Local";
		static string DBA_LocalCnnString = "";
		static string MMCnnString = "";
		static string ProcessErrorHeader = "";
		static string ProcessError = "";
		static List<string[]> ErrorList = new List<string[]>();
		static int ErrorCount = 0;
		static string HostIP = "127.0.0.1";
		static string HostPort = "14443";
		static string PacketType = "1018"; //Add Tracking Number
		static string eConnectPassword = "L0C0S08";
		static Int32 AlertEmailFrequencyInMinutes = 15;
		static string AlertEmailType = "LCS_ETL_ImportUPSTrackingNumbersIntoMM";
		static string JobName = "";
		static int JobId = 0;
		static CommonModules_Main cm = new CommonModules_Main();
		static ProcessPacket_VB pp = new ProcessPacket_VB();

		static void Main(string[] args)
		{
			DBA_LocalCnnString = cm.SQLConnectionString_Get(DBA_Local_DBName);

			MMCnnString = cm.SQLConnectionString_Get("ManageMore");
			JobId = cm.DBA_ETL_Jobs_GetJobIdForJobName(DBA_LocalCnnString, JobName);

			DataTable dtTrckNbrs = SelectNewTrackingNumbers();

			foreach (DataRow row in dtTrckNbrs.Rows)
			{
				string UPSLogId = row["UPSLogId"].ToString();
				string MMInvoiceNumber = (string)row["InvoiceNbr"].ToString();
				string MMOrderNumber = row["InvoiceNbr"].ToString();
				string TrckNbr = (string)row["TrackingNbr"];
				string Comment = "";
				bool IsUpdate = false;

				Int32 MMInvoiceId;

				if (Int32.TryParse(MMInvoiceNumber, out MMInvoiceId))
				{
					int IsProcessed = ProcessTicket(UPSLogId, MMInvoiceId, MMOrderNumber, TrckNbr, ref Comment);
					if (IsProcessed == 1 ) 
					{
						IsUpdate = true; 
					}
				}
				else
				{
					//ProcessErrorHeader = "ProcessTicket Error";
					//ProcessError = "Invoice number: " + MMInvoiceNumber + " not numeric. (UPSLogId=" + UPSLogId + ")";
					//AddErrorToList(UPSLogId, AlertEmailType + "; ProcessTicket");

					Comment = "Warning. Invoice number not numeric.";
					IsUpdate = true;
				}

				if (IsUpdate)
				{
					UpdateIsProcessed(UPSLogId, Comment);
				}
			}

			if (ErrorCount > 0)
			{
				PrepareAndScheduleAlertEmail();
			}
		}

		static int ProcessTicket(string UPSLogId, Int32 MMInvoiceId, string MMOrderNumber, string TrckNbr, ref string Comment)
		{
			int IsProcessed = 0;
			if (IsMMInvoiceExists(UPSLogId, MMInvoiceId))
			{
				if (!IsMMTrckNbrExistsForTheInvoice(UPSLogId, MMInvoiceId, TrckNbr))
				{
					string PacketStr = PreparePacket(UPSLogId, PacketType, MMInvoiceId.ToString(), MMOrderNumber, TrckNbr);
					int NewOrderId = 0; //don't really need this, but target function requires it

					int PacketResponse = pp.ProcessPacket_Main(MMInvoiceId, "Invoice", HostIP, HostPort, PacketType, PacketStr, true, ref NewOrderId, ref ProcessErrorHeader, ref ProcessError);

					if (PacketResponse != 1 || ProcessError != "") //Error
					{
						AddErrorToList(UPSLogId, AlertEmailType + "; ProcessTicket; ProcessPacket_Main (VB.NET dll)");
					}
					else
					{
						IsProcessed = 1;
						Comment = "Success";
					}
				}
				else
				{
					IsProcessed = 1;
					Comment = "Tracking number already exists for this invoice.";
				}
			}
			else
			{
				ProcessErrorHeader = "ProcessTicket Error";
				ProcessError = "Invoice number: " + MMInvoiceId.ToString() + " not found in ManageMore. (UPSLogId=" + UPSLogId + ")";
				AddErrorToList(UPSLogId, AlertEmailType + "; ProcessTicket");
			}

			return IsProcessed;
		}

		static DataTable SelectNewTrackingNumbers()
		{
			DataTable dt = new DataTable();
			try
			{
				using (SqlConnection cnn = new SqlConnection(DBA_LocalCnnString))
				{
					cnn.Open();
					using (SqlDataAdapter da = new SqlDataAdapter("", cnn))
					{
						da.SelectCommand.CommandText = "select f.UPSLogId, isnull(f.InvoiceNbr,'') as InvoiceNbr, isnull(f.TrackingNbr,'') as TrackingNbr, f.[Date] from UPSLog f (nolock) where f.IsProcessed = 0";
						da.Fill(dt);
					}
				}
			}
			catch (Exception e)
			{
				ProcessErrorHeader = "SelectNewTrackingNumbers Error";
				ProcessError = e.Message;
				AddErrorToList("0", AlertEmailType + "; SelectNewTrackingNumbers");
			}


			return dt;
		}

		static bool IsMMInvoiceExists(string UPSLogId, Int32 MMInvoiceId)
		{
			bool isexists = false;

			using (OdbcConnection cnnmm = new OdbcConnection(MMCnnString))
			{
				cnnmm.Open();
				using (OdbcCommand cmdmm = new OdbcCommand("", cnnmm))
				{
					cmdmm.CommandText = "SELECT t.TRANSACTIONID FROM \"Transact\" t WHERE t.TRANSACTIONID = " + MMInvoiceId.ToString();
					using (OdbcDataReader rdrmm = cmdmm.ExecuteReader())
					{
						if (rdrmm.HasRows)
						{
							isexists = true;
						}
						else
						{
							ProcessErrorHeader = "IsMMInvoiceExists";
							ProcessError = "Could not find Invoice number: " + MMInvoiceId.ToString() + " in ManageMore. (UPSLogId = " + UPSLogId + ")";
							AddErrorToList(UPSLogId, AlertEmailType + "; IsMMInvoiceExists");
						}
					}
				}
			}

			return isexists;
		}

		static bool IsMMTrckNbrExistsForTheInvoice(string UPSLogId, Int32 MMInvoiceId, string TrckNbr)
		{
			bool isexists = true;

			using (OdbcConnection cnnmm = new OdbcConnection(MMCnnString))
			{
				cnnmm.Open();
				using (OdbcCommand cmdmm = new OdbcCommand("", cnnmm))
				{
					cmdmm.CommandText = @"SELECT Trantrac.TRACKINGNUMBER
																FROM ""Trantrac"" Trantrac
																WHERE Trantrac.TRACKINGNUMBER = '" + TrckNbr + @"'
																AND Trantrac.TRANSACTIONID = " + MMInvoiceId.ToString();

					using (OdbcDataReader rdrmm = cmdmm.ExecuteReader())
					{
						if (!rdrmm.HasRows)
						{
							isexists = false;
						}
					}
				}
			}
			return isexists;
		}

		static string PreparePacket(string UPSLogId, string PacketTypeCode, string MMInvoiceNumber, string MMOrderNumber, string TrckNbr)
		{
			string packet = "";
			try
			{
				packet = PacketTypeCode + "|WID=UniqueValue1|TID=" + MMInvoiceNumber + "|OID=" + MMOrderNumber + "|TRK=" + TrckNbr + "|PPW=" + eConnectPassword + "|D=" + DateTime.Now.ToString("d") + "|T=" + DateTime.Now.ToString("T") + "|" + (char)4;

			}
			catch (Exception e)
			{
				ProcessErrorHeader = "Prepare Packet Error";
				ProcessError = e.Message;
				AddErrorToList(UPSLogId, AlertEmailType + "; PreparePacket");
			}
			return packet;
		}

		static void PrepareAndScheduleAlertEmail()
		{
			string EmailSubject = Environment.MachineName + "; DBA_Local CRITICAL ERROR! " + AlertEmailType + " error!";
			string EmailBody = "<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">" +
						" <html xmlns=\"http://www.w3.org/1999/xhtml\">" +
						"<body>";

			string[][] Errors = ErrorList.ToArray();
			foreach (string[] Err in ErrorList)
			{
				EmailBody += "</br><b>Error Header:</b> " + Err[0] +
											"</br></br><b>Error Content:</b>" +
											"</br>" + Err[1];

			}
			EmailBody += "</body>" +
										"</html>";

			ProcessErrorHeader = "";
			ProcessError = "";

			try
			{
				cm.EmailQueue_Save(AlertEmailFrequencyInMinutes, DBA_LocalCnnString, AlertEmailType, 0, "mmusicki@yahoo.com", "", "", "", EmailSubject, EmailBody, ref ProcessErrorHeader, ref ProcessError, "HTML", false);

				if (ProcessError != "")
				{
					cm.DBA_ETL_Jobs_Log_Save(JobId, ProcessErrorHeader + "; " + ProcessError);
					throw new Exception("Error occured while sending admin email (1). ErrorHeader: " + ProcessErrorHeader + "; Error: " + ProcessError);
				}
			}
			catch (Exception e)
			{
				cm.DBA_ETL_Jobs_Log_Save(JobId, ProcessErrorHeader + "; " + ProcessError);
				throw new Exception("Error occured while sending admin email (2). " + e.Message);
			}
		}

		static void AddErrorToList (string UPSLogId, string MethodPath)
		{
			ProcessErrorHeader = MethodPath + "; " + ProcessErrorHeader;
			ProcessError = "Error. UPSLogId=" + UPSLogId + "; Error; " + ProcessError;
			cm.DBA_ETL_Jobs_Log_Save(JobId, ProcessErrorHeader + "; " + ProcessError);
			string[] err = { ProcessErrorHeader, ProcessError };
			ErrorList.Add(err);
			ErrorCount++;
			ProcessErrorHeader = "";
			ProcessError = "";
		}

		static void UpdateIsProcessed (string UPSLogId, string Comment)
		{
			try
			{
				using (SqlConnection cnn = new SqlConnection(DBA_LocalCnnString))
				{
					cnn.Open();
					using (SqlCommand cmd = new SqlCommand("", cnn))
					{
						cmd.CommandText = @"UPDATE UPSLog SET 
																IsProcessed = 1
																,ProcessedDateTime = dbo.fnESTDate(GETDATE())
																,ProcessingResult = @ProcessingResult
															WHERE UPSLogId = @UPSLogId;";

						cmd.Parameters.AddWithValue("@UPSLogId", UPSLogId);
						cmd.Parameters.AddWithValue("@ProcessingResult", Comment);
						cmd.ExecuteNonQuery();
					}
				}
			}
			catch (Exception e)
			{
				ProcessErrorHeader = "UpdateIsProcessed Error";
				ProcessError = e.Message;
				AddErrorToList(UPSLogId, AlertEmailType + "; UpdateIsProcessed");

			}
		}
	}
}
