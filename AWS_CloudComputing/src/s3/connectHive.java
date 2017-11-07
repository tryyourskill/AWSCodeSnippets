package s3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class connectHive {
	public static void main(String[] args) {

		try {
			Class.forName("com.amazon.hive.jdbc41.HS2Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		Connection conn = null;
		ResultSet rs = null;
		try {
			conn = DriverManager.getConnection("jdbc:hive2://54.144.188.84:10000/default", "hadoop", "");
			Statement stmt = conn.createStatement();
			String addJarQuery = "ADD JAR /path/on/namenode/to/json-serde-1.3-jar-with-dependencies.jar";
			String queryStr = "select * from my_table limit 10";

			stmt.execute(addJarQuery);
			rs = stmt.executeQuery(queryStr);

			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		System.out.println("End of the program");
	}
}
