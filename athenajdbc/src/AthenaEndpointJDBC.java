import java.sql.*;
import java.util.Properties;

public class AthenaEndpointJDBC {
    // static final String athenaUrl = "jdbc:awsathena://AwsRegion=cn-north-1;";
    static final String athenaUrl = "jdbc:awsathena://athena.cn-north-1.amazonaws.com.cn:443;AwsRegion=cn-north-1;";

    public static void main(String[] args) throws Exception {
		Connection conn = null;
		Statement statement = null;
		try {
			Class.forName("com.simba.athena.jdbc.Driver");
			Properties info = new Properties();

			info.put("S3OutputLocation", "s3://ccas20-athena-queryresult-from-endpoing-origin-bucket-20220125/");
			info.put("LogPath", "/Users/myUser/athenaLog");
			info.put("LogLevel","6");
			info.put ("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.PropertiesFileCredentialsProvider");
			// info.put ("AwsCredentialsProviderArguments", "/Users/yinshuo/.aws/athenaCredentials");
			info.put ("AwsCredentialsProviderArguments", "/home/ssm-user/.aws/athenaCredentials");

			String databaseName = "default";
			System.out.println("Connecting to Athena...");
			conn = DriverManager.getConnection(athenaUrl, info);
			System.out.println("Listing tables...");

			String sql = "show tables in "+ databaseName;
			statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(sql);

			while (rs.next()) {
				//Retrieve table column.
				String name = rs.getString("tab_name");
				//Display values.
				System.out.println("Name: " + name);
			}

			rs.close();
			conn.close();

			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (statement != null)
					statement.close();
				} catch (Exception ex) {
				}

				try {
					if (conn != null)
					conn.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
		}
		System.out.println("Finished connectivity test.");
    }
}


