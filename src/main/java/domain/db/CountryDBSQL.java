package domain.db;

import domain.model.Country;
import util.DbConnectionService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CountryDBSQL implements CountryDB {
    private Connection connection;
    private final String schema;

    public CountryDBSQL() {
        this.connection = DbConnectionService.getDbConnection();
        this.schema = DbConnectionService.getSchema();
        System.out.println(this.schema);
    }

    /**
     * Stores the given country in the database
     *
     * @param country The country to be added
     * @throws DbException if the given country is null
     * @throws DbException if the given country can not be added
     */
    @Override
    public void add(Country country) {

        if (country == null) {
            throw new DbException("Nothing to add.");
        }
        String sql = String.format("INSERT INTO %s.country (name, capital, inhabitants, votes) VALUES (?, ?, ?, ?)", this.schema);

        try {
            PreparedStatement statementSQL = getConnection().prepareStatement(sql);
            statementSQL.setString(1, country.getName());
            statementSQL.setString(2, country.getCapital());
            statementSQL.setInt(3, country.getNumberInhabitants());
            statementSQL.setInt(4, country.getVotes());
            statementSQL.execute();
        } catch (SQLException e) {
            throw new DbException(e);
        }
    }

    @Override
    /**
     * Returns a list with all countries stored in the database
     * @return An arraylist with all countries stored in the database
     * @throws DbException when there are problems with the connection to the database
     */
    public List<Country> getAll() {

        List<Country> countries = new ArrayList<Country>();
        String sql = String.format("SELECT * FROM %s.country", this.schema);
        try {
            PreparedStatement statementSql = getConnection().prepareStatement(sql);
            ResultSet result = statementSql.executeQuery();
            while (result.next()) {
                String name = result.getString("name");
                String capital = result.getString("capital");
                int numberOfVotes = Integer.parseInt(result.getString("votes"));
                int numberOfInhabitants = Integer.parseInt(result.getString("inhabitants"));
                Country country = new Country(name, numberOfInhabitants, capital, numberOfVotes);
                countries.add(country);
            }
        } catch (SQLException e) {
            throw new DbException(e.getMessage(), e);
        }
        return countries;
    }

    /**
     * Check the connection and reconnect when necessery
     *
     * @return the connection with the db, if there is one
     */
    private Connection getConnection() {
        checkConnection();
        return this.connection;
    }

    /**
     * Check if the connection is still open
     * When connection has been closed: reconnect
     */
    private void checkConnection() {
        try {
            if (this.connection == null || this.connection.isClosed()) {
                System.out.println("Connection has been closed");
                this.reConnect();
            }
        } catch (SQLException throwables) {
            throw new DbException(throwables.getMessage());
        }
    }

    /**
     * Reconnects application to db
     */
    private void reConnect() {
        DbConnectionService.disconnect();   // close connection with db properly
        DbConnectionService.reconnect();      // reconnect application to db server
        this.connection = DbConnectionService.getDbConnection();    // assign connection to DBSQL
    }


}
