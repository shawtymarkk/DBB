package com.company;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main extends Configs {

    static Connection yDbConnection;
    static Connection anDbConnection;


    public static void main(String[] args) throws SQLException, ParseException {

        try {
            yDbConnection = getDbConnection(yDBHost, yDBPort, yDBUser, yDBPass, yDBName);
            System.out.println("Успешно подключились к бд " + yDbConnection.getCatalog());
            anDbConnection = getDbConnection(anDBHost, anDBPort, anDBUser, anDBPass, anDBName);
            System.out.println("Успешно подключились к бд " + anDbConnection.getCatalog());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        int replacedAccounts = replacingAccounts();
        System.out.println("Перенесли и заменили: " + replacedAccounts + " аккаунтов");
        yDbConnection.close();
        anDbConnection.close();
    }

    public static Connection getDbConnection(String dbHost, String dbPort, String dbUser, String dbPass, String dbName)
            throws SQLException, ClassNotFoundException {

        String stringConnection = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;

        Class.forName("com.mysql.cj.jdbc.Driver");

        Connection dbConnection = DriverManager.getConnection(stringConnection, dbUser, dbPass);

        return dbConnection;
    }

    public static int replacingAccounts() throws SQLException, ParseException {

        int replacedAccounts = 0;
        String selectAll = "SELECT * FROM players";
        ResultSet anResultSet = anDbConnection.prepareStatement(selectAll).executeQuery();

        while(anResultSet.next()) {
            String selectString = "SELECT * FROM players WHERE " + Const.NAME +" = ?";
            PreparedStatement prSt = yDbConnection.prepareStatement(selectString);
            prSt.setString(1, anResultSet.getString(Const.NAME));
            ResultSet yResultSet = prSt.executeQuery();
            if(yResultSet.next()) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
                simpleDateFormat.applyPattern("yyyy-MM-dd HH:mm:ss");
                Date yLastOnline = simpleDateFormat.parse(yResultSet.getString(Const.LAST_LOGIN));
                Date anLastOnline = simpleDateFormat.parse(anResultSet.getString(Const.LAST_LOGIN));
                if(yLastOnline.before(anLastOnline)) {
                    String updateString = "UPDATE players SET "
                            + Const.NAME + " = ?, "
                            + Const.LAST_IP + " = ?, "
                            + Const.REG_IP + " = ?, "
                            + Const.LAST_LOGIN + " = ?, "
                            + Const.REG_LOGIN + " = ?, "
                            + Const.EMAIL + " = ?, "
                            + Const.ONLINE_SEC + " = ?, "
                            + Const.AUTH_TYPE + " = ?, "
                            + Const.PREMIUM + " = ?, "
                            + Const.PASSWORD + " = ?, "
                            + Const.LAST_VERSION + " = ?, "
                            + Const.REG_VERSION + " = ?, "
                            + Const.LAST_SERVER_IP + " = ?, "
                            + Const.REG_SERVER_IP + " = ?, "
                            + Const.LAST_LOCALE + " = ?, "
                            + Const.REG_LOCALE + " = ?, "
                            + Const.LAST_VIEW_DISTANCE + " = ?, "
                            + Const.REG_VIEW_DISTANCE + " = ? WHERE " + Const.NAME + " = ?)";
                    PreparedStatement preparedStatement = yDbConnection.prepareStatement(updateString);
                    preparedStatement.setString(1, anResultSet.getString(Const.NAME));
                    preparedStatement.setString(2, anResultSet.getString(Const.LAST_IP));
                    preparedStatement.setString(3, anResultSet.getString(Const.REG_IP));
                    preparedStatement.setString(4, anResultSet.getString(Const.LAST_LOGIN));
                    preparedStatement.setString(5, anResultSet.getString(Const.REG_LOGIN));
                    preparedStatement.setString(6, anResultSet.getString(Const.EMAIL));
                    preparedStatement.setString(7, anResultSet.getString(Const.ONLINE_SEC));
                    preparedStatement.setString(8, anResultSet.getString(Const.AUTH_TYPE));
                    preparedStatement.setInt(9, anResultSet.getInt(Const.PREMIUM));
                    preparedStatement.setString(10, anResultSet.getString(Const.PASSWORD));
                    preparedStatement.setString(11, anResultSet.getString(Const.LAST_VERSION));
                    preparedStatement.setString(12, anResultSet.getString(Const.REG_VERSION));
                    preparedStatement.setString(13, anResultSet.getString(Const.LAST_SERVER_IP));
                    preparedStatement.setString(14, anResultSet.getString(Const.REG_SERVER_IP));
                    preparedStatement.setString(15, anResultSet.getString(Const.LAST_LOCALE));
                    preparedStatement.setString(16, anResultSet.getString(Const.REG_LOCALE));
                    preparedStatement.setString(17, anResultSet.getString(Const.LAST_VIEW_DISTANCE));
                    preparedStatement.setString(18, anResultSet.getString(Const.REG_VIEW_DISTANCE));
                    preparedStatement.setString(19, anResultSet.getString(Const.NAME));
                    preparedStatement.executeUpdate();
                    replacedAccounts++;
                }
            } else {
                String updateString = "INSERT INTO players ("
                        + Const.NAME + ", "
                        + Const.LAST_IP + ", "
                        + Const.REG_IP + ", "
                        + Const.LAST_LOGIN + ", "
                        + Const.REG_LOGIN + ", "
                        + Const.EMAIL + ", "
                        + Const.ONLINE_SEC + ", "
                        + Const.AUTH_TYPE + ", "
                        + Const.PREMIUM + ", "
                        + Const.PASSWORD + ", "
                        + Const.LAST_VERSION + ", "
                        + Const.REG_VERSION + ", "
                        + Const.LAST_SERVER_IP + ", "
                        + Const.REG_SERVER_IP + ", "
                        + Const.LAST_LOCALE + ", "
                        + Const.REG_LOCALE + ", "
                        + Const.LAST_VIEW_DISTANCE + ", "
                        + Const.REG_VIEW_DISTANCE +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStatement = yDbConnection.prepareStatement(updateString);
                preparedStatement.setString(1, anResultSet.getString(Const.NAME));
                preparedStatement.setString(2, anResultSet.getString(Const.LAST_IP));
                preparedStatement.setString(3, anResultSet.getString(Const.REG_IP));
                preparedStatement.setString(4, anResultSet.getString(Const.LAST_LOGIN));
                preparedStatement.setString(5, anResultSet.getString(Const.REG_LOGIN));
                preparedStatement.setString(6, anResultSet.getString(Const.EMAIL));
                preparedStatement.setString(7, anResultSet.getString(Const.ONLINE_SEC));
                preparedStatement.setString(8, anResultSet.getString(Const.AUTH_TYPE));
                preparedStatement.setInt(9, anResultSet.getInt(Const.PREMIUM));
                preparedStatement.setString(10, anResultSet.getString(Const.PASSWORD));
                preparedStatement.setString(11, anResultSet.getString(Const.LAST_VERSION));
                preparedStatement.setString(12, anResultSet.getString(Const.REG_VERSION));
                preparedStatement.setString(13, anResultSet.getString(Const.LAST_SERVER_IP));
                preparedStatement.setString(14, anResultSet.getString(Const.REG_SERVER_IP));
                preparedStatement.setString(15, anResultSet.getString(Const.LAST_LOCALE));
                preparedStatement.setString(16, anResultSet.getString(Const.REG_LOCALE));
                preparedStatement.setString(17, anResultSet.getString(Const.LAST_VIEW_DISTANCE));
                preparedStatement.setString(18, anResultSet.getString(Const.REG_VIEW_DISTANCE));
                preparedStatement.executeUpdate();
                replacedAccounts++;
            }
        }
        return replacedAccounts;
    }

}
