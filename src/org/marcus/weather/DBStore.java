package org.marcus.weather;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DBStore {

	// table names
	private static final String hourlyActualTable = "hourly_actual";
	private static final String dailyForecastTable = "daily_forecast";
	private static final String hourlyForecastTable = "hourly_forecast";
	private static final String dailyActualTable = "daily_actual";
	private static final String zipsTable = "zips_collected";

	private static final String selectQuery = "SELECT * FROM weather.dbo.";
	private static final String insertQuery = "INSERT INTO weather.dbo.";
	private static final String updateQuery = "UPDATE weather.dbo.";

	// query bodies
	private static final String hourlyActualUpdateBody = " SET temp=?, conditions=?, precip_amount=? WHERE zip=? AND collected_date=? AND occurred_date=? AND hour=?";
	private static final String dailyForecastUpdateBody = " SET high=?, precip_chance=? WHERE zip=? AND collected_date=? AND forecast_date=?";
	private static final String hourlyForecastUpdateBody = " SET temp=?, precip_chance=? WHERE zip=? AND collected_date=? AND forecast_date=? AND hour=?";
	private static final String dailyActualUpdateBody = " SET high=?, precip_amount=? WHERE zip=? AND collected_date=? AND occurred_date=?";

	private static final String hourlyActualInsertBody = " (zip,collected_time,collected_date,occurred_date,hour,temp,conditions,precip_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String dailyForecastInsertBody = " (zip,collected_time,collected_date,forecast_date,high,precip_chance,delta_high) VALUES (?,?,?,?,?,?,?)";
	private static final String hourlyForecastInsertBody = " (zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance) VALUES (?,?,?,?,?,?,?)";
	private static final String dailyActualInsertBody = " (zip,collected_time,collected_date,occurred_date,high,precip_amount,delta_high) VALUES (?,?,?,?,?,?,?)";

	private static final String zipsCollectedQueryBody = " WHERE collected_date=? AND zip=?";
	private static final String zipsCollectedInsertBody = " (collected_date,zip,tries,successful) VALUES (?,?,?,?)";
	private static final String zipsCollectedUpdateBody = " SET tries=?, successful=? WHERE collected_date=? AND zip=?";

	private String hourlyActualUpdate;
	private String dailyForecastUpdate;
	private String hourlyForecastUpdate;
	private String dailyActualUpdate;

	private String hourlyActualInsert;
	private String dailyForecastInsert;
	private String hourlyForecastInsert;
	private String dailyActualInsert;

	private String zipsCollectedQuery;
	private String zipsCollectedInsert;
	private String zipsCollectedUpdate;

	private Boolean overwrite = null;
	private Connection con;

	private PreparedStatement insertHA;
	private PreparedStatement insertDA;
	private PreparedStatement insertHF;
	private PreparedStatement insertDF;

	private PreparedStatement updateHA;
	private PreparedStatement updateDA;
	private PreparedStatement updateHF;
	private PreparedStatement updateDF;

	private PreparedStatement selectZC;
	private PreparedStatement insertZC;
	private PreparedStatement updateZC;

	private String today;
	private boolean normalTableNames = true;
	
	public DBStore(boolean overwrite, boolean writeToNormalTables) {
		this.normalTableNames = writeToNormalTables;
		this.overwrite = overwrite;
		String filler = "";
		if (!writeToNormalTables) {
			filler = "_alt";
		}
		init(filler);
	}

	public DBStore(boolean overwrite) {
		this.overwrite = overwrite;
		String filler = "";
		if (!normalTableNames) {
			filler = "_alt";
		}
		init(filler);
	}

	private void init(String filler) {
		hourlyActualUpdate = updateQuery + hourlyActualTable + filler
				+ hourlyActualUpdateBody;
		dailyForecastUpdate = updateQuery + dailyForecastTable + filler
				+ dailyForecastUpdateBody;
		hourlyForecastUpdate = updateQuery + hourlyForecastTable + filler
				+ hourlyForecastUpdateBody;
		dailyActualUpdate = updateQuery + dailyActualTable + filler
				+ dailyActualUpdateBody;

		hourlyActualInsert = insertQuery + hourlyActualTable + filler
				+ hourlyActualInsertBody;
		dailyForecastInsert = insertQuery + dailyForecastTable + filler
				+ dailyForecastInsertBody;
		hourlyForecastInsert = insertQuery + hourlyForecastTable + filler
				+ hourlyForecastInsertBody;
		dailyActualInsert = insertQuery + dailyActualTable + filler
				+ dailyActualInsertBody;

		zipsCollectedQuery = selectQuery + zipsTable + filler
				+ zipsCollectedQueryBody;
		zipsCollectedInsert = insertQuery + zipsTable + filler
				+ zipsCollectedInsertBody;
		zipsCollectedUpdate = updateQuery + zipsTable + filler
				+ zipsCollectedUpdateBody;
	}

	public void open() throws ClassNotFoundException, SQLException {
		if (overwrite == null)
			overwrite = false;
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://FRENUM\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		con = DriverManager.getConnection(connectionURL);
		con.setAutoCommit(false);
		insertHA = con.prepareStatement(hourlyActualInsert,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		insertDA = con.prepareStatement(dailyActualInsert,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		insertHF = con.prepareStatement(hourlyForecastInsert,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		insertDF = con.prepareStatement(dailyForecastInsert,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		updateHA = con.prepareStatement(hourlyActualUpdate,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		updateDA = con.prepareStatement(dailyActualUpdate,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		updateHF = con.prepareStatement(hourlyForecastUpdate,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		updateDF = con.prepareStatement(dailyForecastUpdate,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		selectZC = con.prepareStatement(zipsCollectedQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		insertZC = con.prepareStatement(zipsCollectedInsert,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		updateZC = con.prepareStatement(zipsCollectedUpdate,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

		today = (new SimpleDateFormat("yyyy-MM-dd")).format((Calendar
				.getInstance()).getTimeInMillis());
	}

	public synchronized void storeForecast(ForecastData forecast) throws Exception {
		if (forecast.hourlyForecast != null) {

			if (forecast.hourlyForecast != null) {
				if (forecast.zip.equalsIgnoreCase("denver,co"))
					forecast.zip = "80201";
				for (int i = 0; i < forecast.hourlyForecast.length; i++) {
					try {
						insertHF.setString(1, forecast.zip);
						insertHF.setTime(2, new java.sql.Time(Calendar
								.getInstance().getTime().getTime()));
						insertHF.setString(3, forecast.today);
						insertHF.setString(4, forecast.forecastDate);
						insertHF.setInt(5, forecast.hourlyForecast[i].hour);
						if (forecast.hourlyForecast[i].temp == null) {
							insertHF.setNull(6, java.sql.Types.INTEGER);
						} else {
							insertHF.setInt(6, forecast.hourlyForecast[i].temp);
						}
						if (forecast.hourlyForecast[i].PoP == null) {
							insertHF.setNull(7, java.sql.Types.INTEGER);
						} else {
							insertHF.setInt(7, forecast.hourlyForecast[i].PoP);
						}
						insertHF.executeUpdate();
					} catch (Exception e) {
						if (e.getMessage() != null) {
							if (e.getMessage().contains("PRIMARY KEY")) {
								updateHF.setObject(3, forecast.zip);
								updateHF.setObject(4, forecast.today);
								updateHF.setObject(5, forecast.forecastDate);
								updateHF.setObject(6, forecast.hourlyForecast[i].hour);
								updateHF.setObject(1, forecast.hourlyForecast[i].temp);
								updateHF.setObject(2, forecast.hourlyForecast[i].PoP);
								updateHF.executeUpdate();
							} else if (e instanceof NullPointerException) {

							} else {
								throw e;
							}
						}
					}
				}
			}
		}
		if (forecast.overallForecast != null) {
			try {
				insertDF.setString(1, forecast.zip);
				insertDF.setTime(2, new java.sql.Time(Calendar.getInstance()
						.getTime().getTime()));
				insertDF.setString(3, forecast.today);
				insertDF.setString(4, forecast.forecastDate);
				if (forecast.overallForecast.high == null) {
					insertDF.setNull(5, java.sql.Types.INTEGER);
				} else {
					insertDF.setInt(5, forecast.overallForecast.high);
				}
				if (forecast.overallForecast.PoP == null) {
					insertDF.setNull(6, java.sql.Types.INTEGER);
				} else {
					insertDF.setInt(6, forecast.overallForecast.PoP);
				}
				insertDF.setNull(7, java.sql.Types.BOOLEAN);
				insertDF.executeUpdate();
			} catch (Exception e) {
				if (e.getMessage() != null) {
					if (e.getMessage().contains("PRIMARY KEY")) {
						updateDF.setObject(3, forecast.zip);
						updateDF.setObject(4, forecast.today);
						updateDF.setObject(5, forecast.forecastDate);
						updateDF.setObject(1, forecast.overallForecast.high);
						updateDF.setObject(2, forecast.overallForecast.PoP);
						updateDF.executeUpdate();
					} else if (e.getClass().equals(new NullPointerException())) {

					} else {
						throw e;
					}
				}
			}
		}
	}

	public synchronized void storePast(PastData past) throws Exception {
		if (past.hourlyPast != null) {
			if (past.hourlyPast != null) {
				if (past.zip.equalsIgnoreCase("denver,co"))
					past.zip = "80201";
				for (int i = 0; i < past.hourlyPast.length; i++) {
					try {
						insertHA.setString(1, past.zip);
						insertHA.setTime(2, new java.sql.Time(Calendar
								.getInstance().getTime().getTime()));
						insertHA.setString(3, past.today);
						insertHA.setString(4, past.occurredDate);
						insertHA.setInt(5, past.hourlyPast[i].hour);
						if (past.hourlyPast[i].temp == null) {
							insertHA.setNull(6, Types.NUMERIC);
						} else {
							insertHA.setFloat(6, past.hourlyPast[i].temp);
						}

						if (past.hourlyPast[i].conditions.isEmpty()) {
							insertHA.setNull(7, Types.VARCHAR);
						} else {
							insertHA.setString(7, past.hourlyPast[i].conditions);
						}

						if (past.hourlyPast[i].precip == null) {
							insertHA.setNull(8, Types.NUMERIC);
						} else {
							insertHA.setFloat(8, past.hourlyPast[i].precip);
						}
						insertHA.executeUpdate();
					} catch (Exception e) {
						if (e.getMessage() != null) {
							if (e.getMessage().contains("PRIMARY KEY")) {
								updateHA.setObject(4, past.zip);
								updateHA.setObject(5, past.today);
								updateHA.setObject(6, past.occurredDate);
								updateHA.setObject(7, past.hourlyPast[i].hour);
								updateHA.setObject(1, past.hourlyPast[i].temp);
								updateHA.setObject(2, past.hourlyPast[i].conditions);
								updateHA.setObject(3, past.hourlyPast[i].precip);
								updateHA.executeUpdate();
							} else if (e.getClass().equals(
									new NullPointerException())) {

							} else {
								throw e;
							}
						}
					}
				}
			}
		}
		if (past.overallPast != null) {
			try {
				insertDA.setString(1, past.zip);
				insertDA.setTime(2, new java.sql.Time(Calendar.getInstance()
						.getTime().getTime()));
				insertDA.setString(3, past.today);
				insertDA.setString(4, past.occurredDate);
				if (past.overallPast.high != null) {
					insertDA.setFloat(5, past.overallPast.high);
				} else {
					insertDA.setNull(5, java.sql.Types.FLOAT);
				}

				if (past.overallPast.precip != null) {
					insertDA.setFloat(6, past.overallPast.precip);
				} else {
					insertDA.setNull(6, java.sql.Types.FLOAT);
				}
				insertDA.setNull(7, java.sql.Types.BOOLEAN);
				insertDA.executeUpdate();
			} catch (Exception e) {
				if (e.getMessage() != null) {
					if (e.getMessage().contains("PRIMARY KEY")) {
						updateDA.setObject(3, past.zip);
						updateDA.setObject(4, past.today);
						updateDA.setObject(5, past.occurredDate);
						updateDA.setObject(1, past.overallPast.high);
						updateDA.setObject(2, past.overallPast.precip);
						updateDA.executeUpdate();
					} else if (e.getClass().equals(new NullPointerException())) {

					} else {
						throw e;
					}
				}
			}
		}
	}

	public synchronized void commit() throws SQLException {
		con.commit();
	}

	public void close() throws SQLException {
		insertHA.close();
		insertDA.close();
		con.close();
	}

	public synchronized void updateZipSuccess(String zip) throws SQLException {
		updateSuccessfulness(zip, 1);
	}

	public synchronized void updateZipFail(String zip) throws SQLException {
		updateSuccessfulness(zip, 0);
	}

	/**
	 * @param zip
	 * @throws SQLException
	 */
	private void updateSuccessfulness(String zip, int succ) throws SQLException {
		int tries = 1;
		if (zip.equalsIgnoreCase("denver,co"))
			zip = "80201";
		try {
			selectZC.setString(1, today);
			selectZC.setString(2, zip);
			ResultSet rs = selectZC.executeQuery();
			if (rs.next()) {
				tries = rs.getInt(3) + 1;
			}
		} catch (Exception e) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		try {
			if (tries == 1) {
				insertZC.setString(1, today);
				insertZC.setString(2, zip);
				insertZC.setInt(3, tries);
				insertZC.setInt(4, succ);
				insertZC.executeUpdate();
			} else {
				updateZC.setString(3, today);
				updateZC.setString(4, zip);
				updateZC.setObject(1, tries);
				updateZC.setObject(2, succ);
				updateZC.executeUpdate();
			}
		} catch (Exception e) {
			if (e.getMessage() != null) {
				if (e.getClass().equals((new NullPointerException()).getClass())){

				} else {
					throw e;
				}
			}
		}
	}

}
