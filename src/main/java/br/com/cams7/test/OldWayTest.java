package br.com.cams7.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.modelmapper.ModelMapper;

@Slf4j
public class OldWayTest {

  private static final boolean SHOW_LOGS = true;

  private static final Map<Integer, Boolean> SHOW_TESTS;

  static {
    SHOW_TESTS = new HashMap<>();
    SHOW_TESTS.put(1, true);
    SHOW_TESTS.put(2, true);
    SHOW_TESTS.put(3, true);
    SHOW_TESTS.put(4, true);
    SHOW_TESTS.put(5, true);
  }

  public static void main(String[] args) {
    final OldWayTest app = new OldWayTest();
    if (SHOW_TESTS.get(1)) {
      System.out.println("1. Registred order:");
      System.out.println(app.saveOrder(1l));
    }
    if (SHOW_TESTS.get(2)) {
      System.out.println("2. Registred order with invalid payment:");
      System.out.println(app.saveOrder(2l));
    }
    if (SHOW_TESTS.get(3)) {
      System.out.println("3. Customer not found:");
      System.out.println(app.saveOrder(5l));
    }
    if (SHOW_TESTS.get(4)) {
      System.out.println("4. Customer's card not found:");
      System.out.println(app.saveOrder(4l));
    }
    if (SHOW_TESTS.get(5)) {
      System.out.println("5. Customer cart's items not found:");
      try {
        System.out.println(app.saveOrder(3l));
      } catch (RuntimeException e) {
        System.out.println("5. Error: " + e.getMessage());
      }
    }
  }

  private static final ModelMapper MODEL_MAPPER = new ModelMapper();

  private static final Map<Long, CustomerResponse> CUSTOMERS;

  static {
    CUSTOMERS = new HashMap<>();
    CUSTOMERS.put(1l, new CustomerResponse(1l, "Gael", "Alves"));
    CUSTOMERS.put(2l, new CustomerResponse(2l, "Edson", "Brito"));
    CUSTOMERS.put(3l, new CustomerResponse(3l, "Elaine", "Teixeira"));
    CUSTOMERS.put(4l, new CustomerResponse(4l, "Stella", "Paz"));
  }

  private static final Map<Long, CustomerCardResponse> CUSTOMER_CARDS;

  static {
    CUSTOMER_CARDS = new HashMap<>();
    CUSTOMER_CARDS.put(1l, new CustomerCardResponse(1l, "5172563238920845"));
    CUSTOMER_CARDS.put(2l, new CustomerCardResponse(2l, "5585470523496195"));
    CUSTOMER_CARDS.put(3l, new CustomerCardResponse(3l, "4916563711189276"));
  }

  private static final Map<Long, List<CartItemResponse>> CART_ITEMS;

  static {
    CART_ITEMS = new HashMap<>();

    List<CartItemResponse> items1 = new ArrayList<>();
    items1.add(new CartItemResponse(1l, 101l, 1, 25.5));
    items1.add(new CartItemResponse(1l, 102l, 2, 10.3));
    items1.add(new CartItemResponse(1l, 103l, 3, 16.8));

    List<CartItemResponse> items2 = new ArrayList<>();
    items2.add(new CartItemResponse(2l, 101l, 2, 25.5));
    items2.add(new CartItemResponse(2l, 102l, 5, 10.3));

    CART_ITEMS.put(1l, items1);
    CART_ITEMS.put(2l, items2);
  }

  private static final Map<Long, Boolean> CUSTOMER_PAYMENTS;

  static {
    CUSTOMER_PAYMENTS = new HashMap<>();
    CUSTOMER_PAYMENTS.put(1l, true);
    CUSTOMER_PAYMENTS.put(2l, false);
  }

  private static final List<OrderModel> ORDERS = new ArrayList<>();

  // Webclient layer
  private Customer getCustomerById(Long customerId) {
    log("1. Get customer by id: customerId={}", customerId);
    final CustomerResponse response = CUSTOMERS.get(customerId);
    if (response == null) return null;
    return new Customer()
        .withCustomerId(customerId)
        .withFullName(String.format("%s %s", response.getFirstName(), response.getLastName()));
  }

  // Webclient layer
  private CustomerCard getCustomerCardByCustomerId(Long customerId) {
    log("2. Get customer's card by customer id: customerId={}", customerId);
    final CustomerCardResponse response = CUSTOMER_CARDS.get(customerId);
    if (response == null) return null;
    return MODEL_MAPPER.map(response, CustomerCard.class);
  }

  // Webclient layer
  private List<CartItem> getCartItemsByCustomerId(Long customerId) {
    log("3. Get customer cart's items by customer id: customerId={}", customerId);
    final List<CartItemResponse> response = CART_ITEMS.get(customerId);
    if (CollectionUtils.isEmpty(response)) return new ArrayList<>();

    final List<CartItem> items = new ArrayList<>();
    response.forEach(
        item -> {
          items.add(
              MODEL_MAPPER
                  .map(item, CartItem.class)
                  .withTotalAmount(item.getUnitPrice() * item.getQuantity()));
        });
    return items;
  }

  // Webclient layer
  private Boolean isValidPaymentByCustomerId(Long customerId) {
    log("5. Is valid payment by customer id: customerId={}", customerId);
    return CUSTOMER_PAYMENTS.get(customerId);
  }

  // Repository layer
  private OrderEntity saveOrder(OrderEntity order) {
    log("4. Save order: order={}", order);
    final CustomerModel customer = MODEL_MAPPER.map(order.getCustomer(), CustomerModel.class);
    final CustomerCardModel card = MODEL_MAPPER.map(order.getCard(), CustomerCardModel.class);
    final List<CartItemModel> items = new ArrayList<>();
    order
        .getItems()
        .forEach(
            item -> {
              items.add(MODEL_MAPPER.map(item, CartItemModel.class));
            });
    final OrderModel model = new OrderModel();
    model.setId(UUID.randomUUID().toString());
    model.setRegistrationDate(order.getRegistrationDate().toLocalDateTime());
    model.setTotal(order.getTotalAmount());
    model.setValidPayment(order.getValidPayment());
    model.setCustomer(customer);
    model.setCard(card);
    model.setItems(items);
    ORDERS.add(model);
    return getOrder(model);
  }

  // Repository layer
  private OrderEntity updatePaymentStatus(String orderId, Boolean validPayment) {
    log("6. Update payment status: orderId={}, validPayment={}", orderId, validPayment);

    for (int i = 0; i < ORDERS.size(); i++) {
      OrderModel model = ORDERS.get(i);
      if (orderId.equals(model.getId())) {
        model.setValidPayment(validPayment);
        return getOrder(model);
      }
    }

    throw new RuntimeException(
        String.format("Some error happened while updating payment status on order '%s'", orderId));
  }

  private static OrderEntity getOrder(OrderModel order) {
    return MODEL_MAPPER
        .map(order, OrderEntity.class)
        .withOrderId(order.getId())
        .withTotalAmount(order.getTotal())
        .withRegistrationDate(order.getRegistrationDate().atZone(ZoneId.of("America/Sao_Paulo")));
  }

  // Core layer
  public OrderEntity saveOrder(Long customerId) {
    final Customer customer = getCustomerById(customerId);
    if (customer == null) return null;

    final CustomerCard card = getCustomerCardByCustomerId(customerId);
    if (card == null) return null;

    final List<CartItem> items = getCartItemsByCustomerId(customerId);
    Collections.sort(items);

    if (CollectionUtils.isEmpty(items))
      throw new RuntimeException("There aren't items in the cart");

    OrderEntity order = new OrderEntity();
    order.setRegistrationDate(ZonedDateTime.now());
    order.setTotalAmount(getTotalAmount(items));
    order.setValidPayment(false);
    order.setCustomer(customer);
    order.setCard(card);
    order.setItems(items);

    order = saveOrder(order);

    final Boolean isValidPayment = isValidPaymentByCustomerId(order.getCustomer().getCustomerId());

    order = updatePaymentStatus(order.getOrderId(), isValidPayment);

    return order;
  }

  private static double getTotalAmount(List<CartItem> items) {
    double totalAmount = 0;
    for (int i = 0; i < items.size(); i++) {
      totalAmount += items.get(i).getTotalAmount();
    }
    return totalAmount;
  }

  private static int compare(CartItem item1, CartItem item2) {
    if (item2.getTotalAmount() > item1.getTotalAmount()) return 1;
    if (item2.getTotalAmount() < item1.getTotalAmount()) return -1;
    return 0;
  }

  private static void log(String message, Object... args) {
    if (SHOW_LOGS) log.info(message, args);
  }

  // Webclient layer
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CustomerResponse {
    private Long id;
    private String firstName;
    private String lastName;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class CustomerModel {
    private Long customerId;
    private String fullName;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Customer {
    private Long customerId;
    private String fullName;
  }

  // Webclient layer
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CustomerCardResponse {
    private Long customerId;
    private String longNum;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class CustomerCardModel {
    private String longNum;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CustomerCard {
    private String longNum;
  }

  // Webclient layer
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CartItemResponse {
    private Long customerId;
    private Long productId;
    private Integer quantity;
    private Double unitPrice;
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class CartItemModel {
    private Long productId;
    private Double totalAmount;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class CartItem implements Comparable<CartItem> {
    private Long productId;
    private Double totalAmount;

    @Override
    public int compareTo(CartItem cart) {
      return compare(this, cart);
    }
  }

  // Repository layer
  @Data
  @NoArgsConstructor
  public static class OrderModel {
    private String id;
    private CustomerModel customer;
    private CustomerCardModel card;
    private List<CartItemModel> items;
    private LocalDateTime registrationDate;
    private Double total;
    private Boolean validPayment;
  }

  // Core layer
  @Data
  @With
  @NoArgsConstructor
  @AllArgsConstructor
  public static class OrderEntity {
    private String orderId;
    private Customer customer;
    private CustomerCard card;
    private List<CartItem> items;
    private ZonedDateTime registrationDate;
    private Double totalAmount;
    private Boolean validPayment;
  }
}
