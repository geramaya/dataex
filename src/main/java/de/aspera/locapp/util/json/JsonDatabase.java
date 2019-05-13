package de.aspera.locapp.util.json;

/**
 * This class will use by JSON read/write operations with google gson and it
 * will be use as dto object to transport database connection informations.
 * 
 * @author adidweis
 *
 */
public class JsonDatabase {

	private String ident;
	private String dbDriver;
	private String dbUrl;
	private String dbUser;
	private String dbPassword;

	public String getIdent() {
		return ident;
	}

	public void setIdent(String ident) {
		this.ident = ident;
	}

	public String getDbDriver() {
		return dbDriver;
	}

	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO implement apache common equalbuilder
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		// TODO implement apache common hashbuilder
		return super.hashCode();
	}
}
