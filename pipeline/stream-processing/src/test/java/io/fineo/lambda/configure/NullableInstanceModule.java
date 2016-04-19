package io.fineo.lambda.configure;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * Allow a single instance module to provide a mock (instead of a null value) that throws an
 * exception if there is any interaction with the provided mock object
 */
public class NullableInstanceModule<T> extends SingleInstanceModule<T> {
  public NullableInstanceModule(T inst, Class<T> clazz) {
    super(inst, clazz);
  }

  @Override
  protected void configure() {
    if (this.inst != null) {
      super.configure();
    } else {
      bind(clazz).toInstance(throwingMock(clazz));
    }
  }

  public static <T> T throwingMock(Class<T> clazz) {
    T inst = Mockito.mock(clazz, (Answer) invocationOnMock -> {
      throw new RuntimeException("Unexpected interaction");
    });
    return inst;
  }
}
