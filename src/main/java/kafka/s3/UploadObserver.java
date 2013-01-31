package kafka.s3;

import java.util.Observable;

public class UploadObserver extends Observable {
	private Long uploads;

	public UploadObserver() {
		uploads = new Long(0);
	}

	public void incrUploads() {
		this.uploads++;
		setChanged();
		notifyObservers();
	}

	public Long getUploads() {
		return uploads;
	}
}
