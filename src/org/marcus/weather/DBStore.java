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

	// query beginnings
	private static final String selectQuery = "SELECT * FROM weather.dbo.";
	private static final String insertQuery = "INSERT INTO weather.dbo.";
	private static final String updateQuery = "UPDATE weather.dbo.";

	// query bodies
	private static final String hourlyActualQueryBody = " WHERE zip=? AND collected_date=? AND occurred_date=? AND hour=?";
	private static final String dailyForecastQueryBody = " WHERE zip=? AND collected_date=? AND forecast_date=?";
	private static final String hourlyForecastQueryBody = " WHERE zip=? AND collected_date=? AND forecast_date=? AND hour=?";
	private static final String dailyActualQueryBody = " WHERE zip=? AND collected_date=? AND occurred_date=?";

	private static final String hourlyActualInsertBody = " (zip,collected_time,collected_date,occurred_date,hour,temp,conditions,precip_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String dailyForecastInsertBody = " (zip,collected_time,collected_date,forecast_date,high,precip_chance,delta_high) VALUES (?,?,?,?,?,?,?)";
	private static final String hourlyForecastInsertBody = " (zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance) VALUES (?,?,?,?,?,?,?)";
	private static final String dailyActualInsertBody = " (zip,collected_time,collected_date,occurred_date,high,precip_amount,delta_high) VALUES (?,?,?,?,?,?,?)";

	private static final String zipsCollectedQueryBody = " WHERE collected_date=? AND zip=?";
	private static final String zipsCollectedInsertBody = " (collected_date,zip,tries,successful) VALUES (?,?,?,?)";
	private static final String zipsCollectedUpdateBody = " SET tries=?, successful=? WHERE collected_date=? AND zip=?";

	private String hourlyActualQuery;
	private String dailyForecastQuery;
	private String hourlyForecastQuery;
	private String dailyActualQuery;

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

	private PreparedStatement overwriteHA;
	private PreparedStatement overwriteDA;
	private PreparedStatement overwriteHF;
	private PreparedStatement overwriteDF;

	private PreparedStatement selectZC;
	private PreparedStatement insertZC;
	private PreparedStatement updateZC;

	private String today;
	private boolean normalTableNames = false;

	public DBStore(boolean overwrite, boolean writeToNormalTables) {
		this.normalTableNames = writeToNormalTables;
		this.overwrite = overwrite;
		String filler = "";
		if (writeToNormalTables) {
			filler = "_alt";
		}
		init(filler);
	}

	public DBStore(boolean overwrite) {
		this.overwrite = overwrite;
		String filler = "";
		if (normalTableNames) {
			filler = "_alt";
		}
		init(filler);
	}

	private void init(String filler) {
		hourlyActualQuery = selectQuery + hourlyActualTable + filler
				+ hourlyActualQueryBody;
		dailyForecastQuery = selectQuery + dailyForecastTable + filler
				+ dailyForecastQueryBody;
		hourlyForecastQuery = selectQuery + hourlyForecastTable + filler
				+ hourlyForecastQueryBody;
		dailyActualQuery = selectQuery + dailyActualTable + filler
				+ dailyActualQueryBody;

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
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
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
		overwriteHA = con.prepareStatement(hourlyActualQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		overwriteDA = con.prepareStatement(dailyActualQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		overwriteHF = con.prepareStatement(hourlyForecastQuery,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		overwriteDF = con.prepareStatement(dailyForecastQuery,
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
								overwriteHF.setObject(1, forecast.zip);
								overwriteHF.setObject(2, forecast.today);
								overwriteHF.setObject(3, forecast.forecastDate);
								overwriteHF.setObject(4,
										forecast.hourlyForecast[i].hour);
								ResultSet rs = overwriteHF.executeQuery();
								rs.next();
								rs.updateObject("temp",
										forecast.hourlyForecast[i].temp);
								rs.updateObject("precip_chance",
										forecast.hourlyForecast[i].PoP);
								rs.updateRow();
								rs.close();
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
						overwriteDF.setObject(1, forecast.zip);
						overwriteDF.setObject(2, forecast.today);
						overwriteDF.setObject(3, forecast.forecastDate);
						ResultSet rs = overwriteDF.executeQuery();
						rs.next();
						rs.updateObject("high", forecast.overallForecast.high);
						rs.updateObject("precip_chance",
								forecast.overallForecast.PoP);
						rs.updateRow();
						rs.close();
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
								overwriteHA.setObject(1, past.zip);
								overwriteHA.setObject(2, past.today);
								overwriteHA.setObject(3, past.occurredDate);
								overwriteHA.setObject(4,
										past.hourlyPast[i].hour);
								ResultSet rs = overwriteHA.executeQuery();
								rs.next();
								rs.updateObject("temp", past.hourlyPast[i].temp);
								rs.updateObject("conditions",
										past.hourlyPast[i].conditions);
								rs.updateObject("precip_amount",
										past.hourlyPast[i].precip);
								rs.updateRow();
								rs.close();
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
						overwriteDA.setObject(1, past.zip);
						overwriteDA.setObject(2, past.today);
						overwriteDA.setObject(3, past.occurredDate);
						ResultSet rs = overwriteDA.executeQuery();
						rs.next();
						rs.updateObject("high", past.overallPast.high);
						rs.updateObject("precip_amount",
								past.overallPast.precip);
						rs.updateRow();
						rs.close();
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

		}
		try {
			if (tries == 1) {
				insertZC.setString(1, today);
				insertZC.setString(2, zip);
				insertZC.setInt(3, tries);
				insertZC.setInt(4, succ);
				insertZC.executeUpdate();
			} else {
				insertZC.setString(1, today);
				insertZC.setString(2, zip);
				insertZC.setInt(3, tries);
				insertZC.setInt(4, succ);
				insertZC.executeUpdate();
			}
		} catch (SQLException e) {
			if (e.getMessage() != null) {
				if (e.getClass().equals(new NullPointerException())) {

				} else {
					throw e;
				}
			}
		}
	}

}
