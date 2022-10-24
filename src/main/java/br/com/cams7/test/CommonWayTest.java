package br.com.cams7.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.commons.collections4.CollectionUtils;
import org.modelmapper.ModelMapper;

public class CommonWayTest {

  public static void main(String[] args) {
    final var app = new CommonWayTest();
    System.out.println("1. Registred order:");
    System.out.println(app.saveOrder(1l));
    System.out.println("2. Registred order with invalid payment:");
    System.out.println(app.saveOrder(2l));
    System.out.println("3. Customer not found:");
    System.out.println(app.saveOrder(6l));
    System.out.println("4. Customer's card not found:");
    System.out.println(app.saveOrder(5l));
    System.out.println("5. Customer cart's items not found:");
    System.out.println(app.saveOrder(4l));
  }

  private static final ModelMapper MODEL_MAPPER = new ModelMapper();

  private static final Map<Long, CustomerResponse> CUSTOMERS =
      List.of(
              new CustomerResponse(1l, "Gael", "Alves"),
              new CustomerResponse(2l, "Edson", "Brito"),
              new CustomerResponse(3l, "Elaine", "Teixeira"),
              new CustomerResponse(4l, "Stella", "Paz"),
              new CustomerResponse(5l, "Severino", "Galvão"))
          .parallelStream()
          .collect(Collectors.toMap(CustomerResponse::getId, Function.identity()));

  private static Map<Long, CustomerCardResponse> CUSTOMER_CARDS =
      List.of(
              new CustomerCardResponse(1l, "5172563238920845"),
              new CustomerCardResponse(2l, "5585470523496195"),
              new CustomerCardResponse(3l, "4916563711189276"),
              new CustomerCardResponse(4l, "5559232861569823"))
          .parallelStream()
          .collect(Collectors.toMap(CustomerCardResponse::getCustomerId, Function.identity()));

  private static Map<Long, List<CartItemResponse>> CART_ITEMS =
      List.of(
              new CartItemResponse(1l, 101l, 1, 25.5),
              new CartItemResponse(1l, 102l, 2, 10.3),
              new CartItemResponse(1l, 103l, 3, 16.8),
              new CartItemResponse(2l, 101l, 2, 25.5),
              new CartItemResponse(2l, 102l, 5, 10.3))
          .parallelStream()
          .collect(Collectors.groupingBy(CartItemResponse::getCustomerId));

  private static Map<Long, Boolean> CUSTOMER_PAYMENTS = Map.of(1l, true, 2l, false);

  private static List<OrderModel> ORDERS = new ArrayList<>();

  // Webclient layer
  private Customer getCustomerById(Long customerId) {
    final var response = CUSTOMERS.get(customerId);
    if (response == null) return null;
    return new Customer()
        .withCustomerId(customerId)
        .withFullName(String.format("%s %s", response.getFirstName(), response.getLastName()));
  }

  // Webclient layer
  private CustomerCard getCustomerCardByCustomerId(Long customerId) {
    final var response = CUSTOMER_CARDS.get(customerId);
    if (response == null) return null;
    return MODEL_MAPPER.map(response, CustomerCard.class);
  }

  // Webclient layer
  private List<CartItem> getCartItemsByCustomerId(Long customerId) {
    final var response = CART_ITEMS.get(customerId);
    if (CollectionUtils.isEmpty(response)) return List.of();
    return response.parallelStream()
        .map(
            item ->
                MODEL_MAPPER
                    .map(item, CartItem.class)
                    .withTotalAmount(item.getUnitPrice() * item.getQuantity()))
        .collect(Collectors.toList());
  }

  // Webclient layer
  private Boolean isValidPaymentByCustomerId(Long customerId) {
    return CUSTOMER_PAYMENTS.get(customerId);
  }

  // Repository layer
  private OrderEntity saveOrder(OrderEntity order) {
    final var customer = MODEL_MAPPER.map(order.getCustomer(), CustomerModel.class);
    final var card = MODEL_MAPPER.map(order.getCard(), CustomerCardModel.class);
    final var items =
        order.getItems().parallelStream()
            .map(item -> MODEL_MAPPER.map(item, CartItemModel.class))
            .collect(Collectors.toList());
    final var model = new OrderModel();
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
    final var model =
        ORDERS.parallelStream()
            .filter(order -> orderId.equals(order.getId()))
            .findFirst()
            .map(
                order -> {
                  order.setValidPayment(validPayment);
                  return order;
                })
            .orElseThrow(
                () ->
                    new RuntimeException(
                        String.format(
                            "Some error happened while updating payment status on order '%s'",
                            orderId)));
    return getOrder(model);
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
    final var customer = getCustomerById(customerId);
    if (customer == null) return null;

    final var card = getCustomerCardByCustomerId(customerId);
    if (card == null) return null;

    final var items =
        getCartItemsByCustomerId(customerId).parallelStream()
            .sorted(CommonWayTest::compare)
            .collect(Collectors.toList());

    if (CollectionUtils.isEmpty(items))
      throw new RuntimeException("There aren't items in the cart");

    var order = new OrderEntity();
    order.setRegistrationDate(ZonedDateTime.now());
    order.setTotalAmount(getTotalAmount(items));
    order.setValidPayment(false);
    order.setCustomer(customer);
    order.setCard(card);
    order.setItems(items);

    order = saveOrder(order);

    final var isValidPayment = isValidPaymentByCustomerId(order.getCustomer().getCustomerId());

    order = updatePaymentStatus(order.getOrderId(), isValidPayment);

    return order;
  }

  private static double getTotalAmount(List<CartItem> items) {
    return items.parallelStream().mapToDouble(CartItem::getTotalAmount).sum();
  }

  private static int compare(CartItem item1, CartItem item2) {
    if (item2.getTotalAmount() > item1.getTotalAmount()) return 1;
    if (item2.getTotalAmount() < item1.getTotalAmount()) return -1;
    return 0;
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
  public static class CartItem {
    private Long productId;
    private Double totalAmount;
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
