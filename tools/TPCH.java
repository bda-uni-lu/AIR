import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class TPCH {

	public static void main(String[] args) throws Exception {

		Class.forName("org.postgresql.Driver");

		String url = "jdbc:postgresql://localhost/tpch";
		Properties props = new Properties();
		props.setProperty("user", "postgres");
		props.setProperty("password", "123postgres");
		props.setProperty("port", "5432");

		Connection connection = DriverManager.getConnection(url, props);

		Statement stmt = connection.createStatement();

		String[] tables = { "region", "nation", "lineitem", "customer", "partsupp", "part", "supplier", "orders" };
		int[] sizes = { 5, 25, 6001215, 150000, 800000, 200000, 10000, 1500000 };
		String[] queries = { "SELECT * FROM region ORDER BY r_regionkey ASC",
				"SELECT * FROM nation ORDER BY n_regionkey ASC", "SELECT * FROM lineitem ORDER BY l_orderkey ASC",
				"SELECT * FROM customer ORDER BY c_nationkey ASC",
				"SELECT * FROM partsupp ORDER BY ps_partkey ASC, ps_suppkey ASC",
				"SELECT * FROM part ORDER BY p_partkey ASC",
				"SELECT * FROM supplier ORDER BY s_suppkey ASC, s_nationkey ASC",
				"SELECT * FROM orders ORDER BY o_custkey ASC", };

		int q = 0;
		for (String table : tables) {

			System.out.println("\n" + queries[q]);
			ResultSet rset = stmt.executeQuery(queries[q]);
			ResultSetMetaData rsMetaData = rset.getMetaData();

			FileOutputStream writer = new FileOutputStream("./parts16/" + table + ".bin_0");
			int numParts = 16;

			int n = 0, k = 0, f = 0;
			while (rset.next()) {

				if (n == 0) {
					// System.out.print(table + ": ");
					for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
						System.out.println(table + ":" + i + "\t" + rsMetaData.getColumnName(i) + "\t"
								+ rsMetaData.getColumnTypeName(i) + "\t" + rsMetaData.getPrecision(i) + "\t"
								+ rsMetaData.getScale(i));
					}
					for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
						if (rsMetaData.getColumnTypeName(i).startsWith("varchar")) {
							continue;
						} else if (rsMetaData.getColumnTypeName(i).startsWith("int")) {
							byte[] bytes = encodeInt(rset.getInt(i));
							System.out.print(rsMetaData.getColumnName(i) + "[" + bytes.length + "] ");
						} else if (rsMetaData.getColumnTypeName(i).startsWith("numeric")) {
							byte[] bytes = encodeFloat(rset.getFloat(i));
							System.out.print(rsMetaData.getColumnName(i) + "[" + bytes.length + "] ");
						} else if (rsMetaData.getColumnTypeName(i).startsWith("serial")) {
							byte[] bytes = encodeInt(rset.getInt(i));
							System.out.print(rsMetaData.getColumnName(i) + "[" + bytes.length + "] ");
						} else {
							byte[] bytes = rset.getString(i).getBytes();
							System.out.print(rsMetaData.getColumnName(i) + "[" + bytes.length + "] ");
						}
					}
					System.out.println();
				}

				for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
					if (rsMetaData.getColumnTypeName(i).startsWith("varchar")) {
						continue;
					} else if (rsMetaData.getColumnTypeName(i).startsWith("int")) {
						byte[] bytes = encodeInt(rset.getInt(i));
						// System.out.print(rset.getInt(i) + " [" + bytes.length
						// + "]\t");
						writer.write(bytes);
					} else if (rsMetaData.getColumnTypeName(i).startsWith("numeric")) {
						byte[] bytes = encodeFloat(rset.getFloat(i));
						// System.out.print(rset.getFloat(i) + " [" +
						// bytes.length + "]\t");
						writer.write(bytes);
					} else if (rsMetaData.getColumnTypeName(i).startsWith("serial")) {
						byte[] bytes = encodeInt(rset.getInt(i));
						// System.out.print(rset.getInt(i) + " [" + bytes.length
						// + "]\t");
						writer.write(bytes);
					} else {
						byte[] bytes = rset.getString(i).getBytes();
						// System.out.print(rset.getString(i) + " [" +
						// bytes.length + "]\t");
						writer.write(bytes);
					}
				}

				if (n > 0 && n % 100000 == 0)
					System.out.println(n + " ... ");

				n++;

				//System.out.println(
				//		"SIZE:" + sizes[q] + "\tN:" + n + "\tF:" + f + "\tK:" + k + "\t" + (sizes[q] / numParts));

				if (++k >= sizes[q] / numParts && f < numParts - 1 && n < sizes[q]) {
					writer.flush();
					writer.close();
					k = 0;
					f++;
					writer = new FileOutputStream("./parts16/" + table + ".bin_" + f);
				}
			}

			System.out.println(table + ": " + n + " TUPLES.");

			writer.flush();
			writer.close();

			q++;
			//if (q > 1)
			//	break;
		}
	}

	static byte[] encodeInt(int value) {
		return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
	}

	static byte[] encodeFloat(float value) {
		return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
	}
}